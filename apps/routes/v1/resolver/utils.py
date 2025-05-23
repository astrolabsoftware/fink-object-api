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
import io
import requests
import pandas as pd
from numpy import unique as npunique

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import hbase_to_dict

from line_profiler import profile


@profile
def resolve_name(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/resolver

    Parameters
    ----------
    payload: dict
        See https://api.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    resolver = payload["resolver"]
    name = payload["name"]
    if "nmax" in payload:
        nmax = payload["nmax"]
    else:
        nmax = 10

    reverse = False
    if "reverse" in payload:
        if payload["reverse"] is True:
            reverse = True

    if resolver == "tns":
        client = connect_to_hbase_table("ztf.tns_resolver")
        client.setLimit(nmax)
        if name == "":
            # return the full table
            results = client.scan(
                "",
                "",
                "*",
                0,
                False,
                False,
            )
        elif reverse:
            # Prefix search on second part of the key which is `fullname_internalname`
            to_evaluate = f"key:key:_{name}:substring"
            results = client.scan(
                "",
                to_evaluate,
                "*",
                0,
                False,
                False,
            )
        else:
            # indices are case-insensitive
            to_evaluate = f"key:key:{name.lower()}"
            results = client.scan(
                "",
                to_evaluate,
                "*",
                0,
                False,
                False,
            )

        # Restore default limits
        client.close()

        pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")
    elif resolver == "simbad":
        client = connect_to_hbase_table("ztf")
        if reverse:
            to_evaluate = f"key:key:{name}"
            client.setLimit(nmax)
            results = client.scan(
                "",
                to_evaluate,
                "i:objectId,d:cdsxmatch,i:ra,i:dec,i:candid,i:jd",
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
            # ZTF alerts -> ssnmanenr
            client = connect_to_hbase_table("ztf")
            to_evaluate = f"key:key:{name}"
            client.setLimit(nmax)
            results = client.scan(
                "",
                to_evaluate,
                "i:objectId,i:ssnamenr",
                0,
                False,
                False,
            )
            client.close()
            pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")

            # ssnmanenr -> MPC name & number
            if not pdf.empty:
                client = connect_to_hbase_table("ztf.sso_resolver")
                ssnamenrs = npunique(pdf["i:ssnamenr"].to_numpy())
                results = {}
                for ssnamenr in ssnamenrs:
                    result = client.scan(
                        "",
                        f"i:ssnamenr:{ssnamenr}:exact",
                        "i:number,i:name,i:ssnamenr",
                        0,
                        False,
                        False,
                    )
                    results.update(result)
                client.close()
                pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")
        else:
            # MPC -> ssnamenr
            # keys follow the pattern <name>-<deduplication>
            client = connect_to_hbase_table("ztf.sso_resolver")

            if nmax == 1:
                # Prefix with internal marker
                to_evaluate = f"key:key:{name.lower()}-"
            elif nmax > 1:
                # This enables e.g. autocompletion tasks
                client.setLimit(nmax)
                to_evaluate = f"key:key:{name.lower()}"

            results = client.scan(
                "",
                to_evaluate,
                "i:ssnamenr,i:name,i:number",
                0,
                False,
                False,
            )
            client.close()
            pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")

    return pdf
