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
import datetime
import pandas as pd
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

    r = requests.get(
        "{}/SSOBULK/sso_rubin_lc_aggregated_{}.parquet?op=OPEN&user.name={}&namenoderpcaddress={}".format(
            input_args["WEBHDFS"],
            version,
            input_args["USER"],
            input_args["NAMENODE"],
        ),
    )

    if payload.get("output-format", "parquet") != "parquet":
        # Full table in other format than parquet (slow)
        return pd.read_parquet(io.BytesIO(r.content))
    else:
        # Full table in parquet (fast)
        # return the schema of the table
        response = Response(io.BytesIO(r.content), 200)
        response.headers.set("Content-Type", "application/parquet")
        return response
