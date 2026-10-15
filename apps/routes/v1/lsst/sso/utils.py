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
import pandas as pd
import requests
from flask import Response

from fink_utils.sso.miriade import get_miriade_data

# from fink_utils.sso.spins import func_hg1g2_with_spin, estimate_sso_params
from line_profiler import profile

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_lsst_hbase_output


def resolve_packed(n_or_d):
    """Resolve all packed names corresponding to input n_or_d"""
    n_or_d = str(n_or_d)

    # Pure quaero implementation
    r = requests.get(
        f"https://ssp.imcce.fr/webservices/ssodnet/api/resolver.php?-name=EQUAL:{n_or_d}&-mime=json&-from=FINK"
    )
    if r.status_code == 200 and r.json() != []:
        sso_name = r.json()["data"][0]["name"]

        aliases = r.json()["data"][0]["aliases"]
        aliases = [i.strip() for i in aliases.split(",")]

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
        See https://api.lsst.fink-portal.org

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

    with_ephem = False
    if "withEphem" in payload and (
        payload["withEphem"] == "True" or payload["withEphem"] is True
    ):
        with_ephem = True  # noqa: F841

    if truncated and "r:packed_primary_provisional_designation" not in cols:
        # For name resolving, i:ssnamenr must be here
        # In case the user forgot, let's add it silently
        cols += ",r:packed_primary_provisional_designation"
    if truncated and with_ephem and "r:midpointMjdTai" not in cols:
        cols += ",r:midpointMjdTai"
    if truncated and with_ephem and "r:psfFlux" not in cols:
        cols += ",r:psfFlux"

    n_or_d = str(payload["n_or_d"])

    if "," in n_or_d:
        ids = [i.strip() for i in n_or_d.split(",")]
    else:
        ids = [n_or_d.strip()]

    # Get all ssnamenrs
    packed = []
    sso_names = {}
    for id_ in ids:
        sso_name, aliases = resolve_packed(id_)

        if sso_name == "":
            rep = {
                "status": "error",
                "text": f"{id_} is not a valid name or number according to quaero.\n",
            }
            return Response(str(rep), 400)

        if len(aliases) == 0:
            rep = {
                "status": "error",
                "text": f"We have found 0 packed designation in the aliases for the object {id_} according to quaero.\n",
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
    pdf["f:sso_name"] = pdf["r:packed_primary_provisional_designation"].apply(
        lambda x: sso_names[x]
    )

    if with_ephem:
        pdf = get_miriade_data(pdf, survey="lsst", observer="X05", shift=0.0)
        if "i:magpsf_red" not in pdf.columns:
            rep = {
                "status": "error",
                "text": "We could not obtain the ephemerides information. Check Miriade availabilities.",
            }
            return Response(str(rep), 400)

    return pdf
