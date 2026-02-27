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
import io
import requests
import pandas as pd

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import hbase_to_dict
from apps.utils.utils import extract_configuration

from line_profiler import profile


@profile
def resolve_name(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/resolver

    Parameters
    ----------
    payload: dict
        See https://api.lsst.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    resolver = payload["resolver"]
    name = payload["name_or_id"]
    if "nmax" in payload:
        nmax = payload["nmax"]
    else:
        nmax = 10

    reverse = False
    if "reverse" in payload:
        if payload["reverse"] is True:
            reverse = True

    if resolver == "tns":
        if name == "":
            client = connect_to_hbase_table("rubin.tns_resolver")
            client.setLimit(nmax)
            # return the full table
            results = client.scan(
                "",
                "",
                "*",
                0,
                False,
                False,
            )
            pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")
        elif reverse:
            # Search main table
            client = connect_to_hbase_table("rubin.diaSource_static")
            to_evaluate = f"key:key:{name[-3:]}_{name}_"
            results = client.scan(
                "",
                to_evaluate,
                "r:diaObjectId,f:xm_tns_fullname,f:xm_tns_type,f:xm_tns_redshift",
                0,
                False,
                False,
            )
            tmp = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")
            # drop duplicates
            tmp = tmp.drop_duplicates()
            # remove xmatch artifacts on high cadence
            pdf = tmp[tmp["f:xm_tns_fullname"].apply(lambda x: x != "nan" and not pd.isna(x))]
        else:
            # indices are case-insensitive
            # salt is last letter of the name
            to_evaluate = f"key:key:{name.lower()[-1]}_{name.lower()}"
            results = client.scan(
                "",
                to_evaluate,
                "*",
                0,
                False,
                False,
            )
            pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")

        # Restore default limits
        client.close()

    elif resolver == "simbad":
        client = connect_to_hbase_table("rubin.diaObject")
        if reverse:
            to_evaluate = f"key:key:{name}"
            client.setLimit(nmax)
            results = client.scan(
                "",
                to_evaluate,
                "r:diaObjectId,f:main_label_classifier,r:ra,r:dec",
                0,
                False,
                False,
            )
            client.close()
            pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")
        else:
            r = requests.get(
                f"http://cds.unistra.fr/cgi-bin/nph-sesame/-oxp/~S?{name}",
            )

            check = pd.read_xml(io.BytesIO(r.content))
            if "Resolver" in check.columns:
                pdf = pd.read_xml(io.BytesIO(r.content), xpath=".//Resolver")
            else:
                pdf = pd.DataFrame()

    elif resolver == "ssodnet":
        if reverse:
            # ssObjectId -> packed_name
            client = connect_to_hbase_table("rubin.sso_resolver")

            to_evaluate = f"key:key:{name[-3:]}_{name}"
            client.setLimit(nmax)
            results = client.scan(
                "",
                to_evaluate,
                "*",
                0,
                False,
                False,
            )
            client.close()
            pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")
        else:
            # SSO name or number -> ssObjectId
            client = connect_to_hbase_table("rubin.mpc_orbits")

            config = extract_configuration("config.yml")

            r = requests.post(
                "{}/api/v1/sso".format(config["APIURL"]),
                json={
                    "n_or_d": name,
                    "columns": "r:packed_primary_provisional_designation,r:ssObjectId,r:diaSourceId",
                },
            )
            pdf = pd.read_json(io.BytesIO(r.content))

    return pdf
