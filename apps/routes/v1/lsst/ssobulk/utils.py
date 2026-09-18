# Copyright 2026 AstroLab Software
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
import json
import datetime
import pandas as pd
import polars as pl
import requests
import yaml
from flask import Response
from line_profiler import profile


@profile
def get_lc(payload: dict) -> pd.DataFrame:
    """Send the Fink Flat Table

    Data is from /api/v1/ssobulk

    Parameters
    ----------
    payload: dict
        See https://api.lsst.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    # Schema
    schema = payload.get("schema", False)
    if schema:
        SCHEMA = {
            "designation": {
                "type": "str",
                "description": "Official name or provisional designation of the SSO",
            },
            "cra": {"type": "list", "description": "List of RA in degree"},
            "cdec": {"type": "list", "description": "List of DEC in degree"},
            "cband": {"type": "list", "description": "List of filter band as str"},
            "cmidpointMjdTai": {
                "type": "list",
                "description": "List of times MJD (TAI)",
            },
            "cphaseAngle": {
                "type": "list",
                "description": "List of phase angles in degree",
            },
            "cephRa": {
                "type": "list",
                "description": "List of RA ephemerides in degree",
            },
            "cephDec": {
                "type": "list",
                "description": "List of DEC ephemerides in degree",
            },
            "ctopoRange": {
                "type": "list",
                "description": "List of topocentric distances in AU",
            },
            "chelioRange": {
                "type": "list",
                "description": "List of heliocentric distances in AU",
            },
            "cephOffsetRa": {
                "type": "list",
                "description": "List of offsets in RA in degree",
            },
            "cephOffsetDec": {
                "type": "list",
                "description": "List of offsets in DEC in degree",
            },
            "cjdUtc": {"type": "list", "description": "List of times in JD (UTC)"},
            "chelioRa": {"type": "list", "description": "List of Sun RA in degree"},
            "chelioDec": {"type": "list", "description": "List of Sun DEC in degree"},
            "cmagpsf": {"type": "list", "description": "List of difference magnitudes"},
            "csigmapsf": {
                "type": "list",
                "description": "List of difference magnitude error estimates",
            },
            "version": {
                "type": "str",
                "description": "Version of the table as YYYY.MM",
            },
        }
        # return the schema of the table
        response = Response(json.dumps(SCHEMA), 200)
        response.headers.set("Content-Type", "application/json")
        return response

    # Need to profile compared to pyarrow
    with open("config.yml") as f:
        input_args = yaml.load(f, yaml.Loader)

    if "version" in payload:
        version = payload["version"]

        # version needs YYYY.MM
        yyyymm = version.split(".")
        if (len(yyyymm[0]) != 4) or (len(yyyymm[1]) != 2):
            rep = {
                "status": "error",
                "text": "version needs to be YYYY.MM\n",
            }
            return Response(str(rep), 400)
        if version < "2026.08":
            rep = {
                "status": "error",
                "text": "version starts on 2026.08\n",
            }
            return Response(str(rep), 400)
    else:
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        version = f"{now.year}.{now.month:02d}"

    # Get file list
    r = requests.get(
        "{}/SSOBULK/sso_rubin_lc_aggregated_{}.parquet?op=LISTSTATUS&user.name={}&namenoderpcaddress={}".format(
            input_args["WEBHDFS"],
            version,
            input_args["USER"],
            input_args["NAMENODE"],
        ),
    )

    if r.status_code != 200:
        response = Response(r.text, r.status_code)
        return response

    frames = []
    for dic in r.json()["FileStatuses"]["FileStatus"]:
        filename = dic["pathSuffix"]
        if filename.endswith(".parquet"):
            r0 = requests.get(
                "{}/SSOBULK/sso_rubin_lc_aggregated_{}.parquet/{}?op=OPEN&user.name={}&namenoderpcaddress={}".format(
                    input_args["WEBHDFS"],
                    version,
                    filename,
                    input_args["USER"],
                    input_args["NAMENODE"],
                ),
            )
            sub = pl.read_parquet(io.BytesIO(r0.content))
            if "sso_name" in payload:
                matching = sub.filter(
                    pl.col("designation").cast(pl.String) == payload["sso_name"]
                )

                if matching.height > 0:
                    return matching
            else:
                frames.append(sub)

    return pl.concat(frames) if frames else pl.DataFrame()
