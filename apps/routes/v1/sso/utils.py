# Copyright 2024 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import requests
from flask import Response

import pandas as pd
import numpy as np

from apps.utils.utils import (
    download_cutout,
    resolve_sso_name_to_ssnamenr,
    resolve_sso_name,
)
from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_hbase_output

from fink_utils.sso.utils import get_miriade_data
from fink_utils.sso.spins import func_hg1g2_with_spin, estimate_sso_params

from line_profiler import profile


@profile
def extract_sso_data(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/sso

    Parameters
    ----------
    payload: dict
        See https://fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    if "columns" in payload:
        cols = payload["columns"].replace(" ", "")
    else:
        cols = "*"

    if cols == "*":
        truncated = False
    else:
        truncated = True

    with_ephem, with_residuals, with_cutouts = False, False, False
    if "withResiduals" in payload and (
        payload["withResiduals"] == "True" or payload["withResiduals"] is True
    ):
        with_residuals = True
        with_ephem = True
    if "withEphem" in payload and (
        payload["withEphem"] == "True" or payload["withEphem"] is True
    ):
        with_ephem = True
    if "withcutouts" in payload and (
        payload["withcutouts"] == "True" or payload["withcutouts"] is True
    ):
        with_cutouts = True

    n_or_d = str(payload["n_or_d"])

    if "," in n_or_d:
        ids = n_or_d.replace(" ", "").split(",")
        multiple_objects = True
    else:
        ids = [n_or_d.replace(" ", "")]
        multiple_objects = False

    # We cannot do multi-object and phase curve computation
    if multiple_objects and with_residuals:
        rep = {
            "status": "error",
            "text": "You cannot request residuals for a list object names.\n",
        }
        return Response(str(rep), 400)

    # Get all ssnamenrs
    ssnamenrs = []
    ssnamenr_to_sso_name = {}
    ssnamenr_to_sso_number = {}
    for id_ in ids:
        if id_.startswith("C/"):
            start = id_[0:6]
            stop = id_[6:]
            r = requests.get(
                "https://api.ssodnet.imcce.fr/quaero/1/sso?q={} {}&type=Comet".format(
                    start, stop
                )
            )
            if r.status_code == 200 and r.json() != []:
                sso_name = r.json()["data"][0]["name"]
            else:
                sso_name = id_
            sso_number = None
        elif id_.endswith("P"):
            sso_name = id_
            sso_number = None
        else:
            # resolve the name of asteroids using rocks
            sso_name, sso_number = resolve_sso_name(id_)

        if not isinstance(sso_number, int) and not isinstance(sso_name, str):
            rep = {
                "status": "error",
                "text": "{} is not a valid name or number according to quaero.\n".format(
                    n_or_d
                ),
            }
            return Response(str(rep), 400)

        # search all ssnamenr corresponding quaero -> ssnamenr
        if isinstance(sso_name, str):
            new_ssnamenrs = resolve_sso_name_to_ssnamenr(sso_name)
            ssnamenrs = np.concatenate((ssnamenrs, new_ssnamenrs))
        else:
            new_ssnamenrs = resolve_sso_name_to_ssnamenr(sso_number)
            ssnamenrs = np.concatenate((ssnamenrs, new_ssnamenrs))

        for ssnamenr_ in new_ssnamenrs:
            ssnamenr_to_sso_name[ssnamenr_] = sso_name
            ssnamenr_to_sso_number[ssnamenr_] = sso_number

    # Get data from the main table
    client = connect_to_hbase_table("ztf.ssnamenr")
    results = {}
    for to_evaluate in ssnamenrs:
        result = client.scan(
            "",
            f"key:key:{to_evaluate}_",
            cols,
            0,
            True,
            True,
        )
        results.update(result)

    schema_client = client.schema()

    # reset the limit in case it has been changed above
    client.close()

    pdf = format_hbase_output(
        results,
        schema_client,
        group_alerts=False,
        truncated=truncated,
        extract_color=False,
    )

    # Propagate name and number
    pdf["sso_name"] = pdf["i:ssnamenr"].apply(lambda x: ssnamenr_to_sso_name[x])
    pdf["sso_number"] = pdf["i:ssnamenr"].apply(lambda x: ssnamenr_to_sso_number[x])

    if with_cutouts:
        # Extract cutouts
        cutout_kind = payload.get("cutout-kind", "Science")
        if cutout_kind not in ["Science", "Template", "Difference"]:
            rep = {
                "status": "error",
                "text": "`cutout-kind` must be `Science`, `Difference`, or `Template`.\n",
            }
            return Response(str(rep), 400)

        # get cutouts
        colname = "b:cutout{}_stampData".format(cutout_kind)
        cutouts = []
        for _, row in pdf.iterrows():
            cutouts.append(
                download_cutout(row["i:objectId"], row["i:candid"], cutout_kind)
            )
        pdf[colname] = cutouts
        # pdf[colname] = pdf[["i:objectId", "i:candid"]].apply(
        #    lambda x: pd.Series([download_cutout(x.iloc[0], x.iloc[1], cutout_kind)]),
        #    axis=1,
        # )

    if with_ephem:
        # We should probably add a timeout
        # and try/except in case of miriade shutdown
        pdf = get_miriade_data(pdf, sso_colname="sso_name")
        if "i:magpsf_red" not in pdf.columns:
            rep = {
                "status": "error",
                "text": "We could not obtain the ephemerides information. Check Miriade availabilities.",
            }
            return Response(str(rep), 400)

    if with_residuals:
        # get phase curve parameters using
        # the sHG1G2 model

        # Phase angle, in radians
        phase = np.deg2rad(pdf["Phase"].values)

        # Required for sHG1G2
        ra = np.deg2rad(pdf["i:ra"].values)
        dec = np.deg2rad(pdf["i:dec"].values)

        outdic = estimate_sso_params(
            magpsf_red=pdf["i:magpsf_red"].to_numpy(),
            sigmapsf=pdf["i:sigmapsf"].to_numpy(),
            phase=phase,
            filters=pdf["i:fid"].to_numpy(),
            ra=ra,
            dec=dec,
            p0=[15.0, 0.15, 0.15, 0.8, np.pi, 0.0],
            bounds=(
                [0, 0, 0, 3e-1, 0, -np.pi / 2],
                [30, 1, 1, 1, 2 * np.pi, np.pi / 2],
            ),
            model="SHG1G2",
            normalise_to_V=False,
        )

        # check if fit converged else return NaN
        if outdic["fit"] != 0:
            pdf["residuals_shg1g2"] = np.nan
        else:
            # per filter construction of the residual
            pdf["residuals_shg1g2"] = 0.0
            for filt in np.unique(pdf["i:fid"]):
                cond = pdf["i:fid"] == filt
                model = func_hg1g2_with_spin(
                    [phase[cond], ra[cond], dec[cond]],
                    outdic["H_{}".format(filt)],
                    outdic["G1_{}".format(filt)],
                    outdic["G2_{}".format(filt)],
                    outdic["R"],
                    np.deg2rad(outdic["alpha0"]),
                    np.deg2rad(outdic["delta0"]),
                )
                pdf.loc[cond, "residuals_shg1g2"] = (
                    pdf.loc[cond, "i:magpsf_red"] - model
                )

    return pdf
