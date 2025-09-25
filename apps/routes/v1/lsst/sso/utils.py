# Copyright 2025 AstroLab Software
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

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_lsst_hbase_output

# from fink_utils.sso.miriade import get_miriade_data
# from fink_utils.sso.spins import func_hg1g2_with_spin, estimate_sso_params

from line_profiler import profile


def resolve_packed(n_or_d):
    """Resolve all packed names corresponding to input n_or_d"""
    n_or_d = str(n_or_d)
    # check if the object is an asteroid or a comet
    if n_or_d.startswith("C/") or n_or_d.endswith("P"):
        obj_type = "Comet"
    else:
        obj_type = "Asteroid"

    # Pure quaero implementation
    r = requests.get(
        "https://api.ssodnet.imcce.fr/quaero/1/sso?q={}&type={}".format(
            n_or_d.replace(" ", "_"), obj_type
        )
    )
    if r.status_code == 200 and r.json() != []:
        if r.json()["total"] > 0:
            sso_name = r.json()["data"][0]["name"]

            aliases = r.json()["data"][0]["aliases"]

            # The provisional designation stored on the orbit and
            # observations is stored in a 7-character packed format
            aliases = [al for al in aliases if len(al) == 7]

            return sso_name, aliases
    return "", []


@profile
def extract_sso_data(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/sso

    Parameters
    ----------
    payload: dict
        See https://api.fink-portal.org

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

    with_ephem, with_residuals = False, False
    if "withResiduals" in payload and (
        payload["withResiduals"] == "True" or payload["withResiduals"] is True
    ):
        with_residuals = True
        with_ephem = True
    if "withEphem" in payload and (
        payload["withEphem"] == "True" or payload["withEphem"] is True
    ):
        with_ephem = True

    if truncated and "r:mpcDesignation" not in cols:
        # For name resolving, i:ssnamenr must be here
        # In case the user forgot, let's add it silently
        cols += ",r:mpcDesignation"

    n_or_d = str(payload["n_or_d"])

    if "," in n_or_d:
        ids = [i.strip() for i in n_or_d.split(",")]
        multiple_objects = True
    else:
        ids = [n_or_d.strip()]
        multiple_objects = False

    # We cannot do multi-object and phase curve computation
    if multiple_objects and with_residuals:
        rep = {
            "status": "error",
            "text": "You cannot request residuals for a list object names.\n",
        }
        return Response(str(rep), 400)

    # Get all ssnamenrs
    packed = []
    sso_names = {}
    for id_ in ids:
        sso_name, aliases = resolve_packed(id_)

        if sso_name == "":
            rep = {
                "status": "error",
                "text": "{} is not a valid name or number according to quaero.\n".format(
                    id_
                ),
            }
            return Response(str(rep), 400)

        if len(aliases) == 0:
            rep = {
                "status": "error",
                "text": "We have found 0 packed designation in the aliases for the object {} according to quaero.\n".format(
                    id_
                ),
            }
            return Response(str(rep), 400)

        packed += aliases
        for alias in aliases:
            sso_names[alias] = sso_name

    # Get data from the main table
    client = connect_to_hbase_table("rubin.diaSource_sso")
    results = {}
    for element in packed:
        salt = element[1:3]
        result = client.scan(
            "",
            f"key:key:{salt}_{element}_",
            cols,
            0,
            True,
            True,
        )
        results.update(result)

    schema_client = client.schema()

    # reset the limit in case it has been changed above
    client.close()

    if len(results) == 0:
        return pd.DataFrame()

    pdf = format_lsst_hbase_output(
        results,
        schema_client,
        group_alerts=False,
        truncated=truncated,
        extract_color=False,
    )

    # Propagate transformation
    pdf["f:sso_name"] = pdf["r:mpcDesignation"].apply(lambda x: sso_names[x])

    # if with_ephem:
    #     # TODO: In case truncated is True, check (before DB call)
    #     #       the mandatory fields have been requested
    #     # TODO: We should probably add a timeout and try/except
    #     #       in case of miriade shutdown
    #     pdf = get_miriade_data(pdf, sso_colname="sso_name")
    #     if "i:magpsf_red" not in pdf.columns:
    #         rep = {
    #             "status": "error",
    #             "text": "We could not obtain the ephemerides information. Check Miriade availabilities.",
    #         }
    #         return Response(str(rep), 400)

    # if with_residuals:
    #     # TODO: In case truncated is True, check (before DB call)
    #     #       the mandatory fields have been requested

    #     # Get phase curve parameters using the sHG1G2 model
    #     phase = np.deg2rad(pdf["Phase"].values)
    #     ra = np.deg2rad(pdf["i:ra"].values)
    #     dec = np.deg2rad(pdf["i:dec"].values)

    #     outdic = estimate_sso_params(
    #         magpsf_red=pdf["i:magpsf_red"].to_numpy(),
    #         sigmapsf=pdf["i:sigmapsf"].to_numpy(),
    #         phase=phase,
    #         filters=pdf["i:fid"].to_numpy(),
    #         ra=ra,
    #         dec=dec,
    #         p0=[15.0, 0.15, 0.15, 0.8, np.pi, 0.0],
    #         bounds=(
    #             [-3, 0, 0, 3e-1, 0, -np.pi / 2],
    #             [30, 1, 1, 1, 2 * np.pi, np.pi / 2],
    #         ),
    #         model="SHG1G2",
    #         normalise_to_V=False,
    #     )

    #     # check if fit converged else return NaN
    #     if outdic["fit"] != 0:
    #         pdf["residuals_shg1g2"] = np.nan
    #     else:
    #         # per filter construction of the residual
    #         pdf["residuals_shg1g2"] = 0.0
    #         for filt in np.unique(pdf["i:fid"]):
    #             cond = pdf["i:fid"] == filt
    #             model = func_hg1g2_with_spin(
    #                 [phase[cond], ra[cond], dec[cond]],
    #                 outdic["H_{}".format(filt)],
    #                 outdic["G1_{}".format(filt)],
    #                 outdic["G2_{}".format(filt)],
    #                 outdic["R"],
    #                 np.deg2rad(outdic["alpha0"]),
    #                 np.deg2rad(outdic["delta0"]),
    #             )
    #             pdf.loc[cond, "residuals_shg1g2"] = (
    #                 pdf.loc[cond, "i:magpsf_red"] - model
    #             )

    return pdf
