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
from flask import Response

import io
import yaml
import requests
import datetime

import pandas as pd

from fink_utils.sso.ssoft import (
    COLUMNS,
    COLUMNS_HG,
    COLUMNS_HG1G2,
    COLUMNS_SHG1G2,
    COLUMNS_SSHG1G2,
)

from line_profiler import profile


@profile
def get_ssoft(payload: dict) -> pd.DataFrame:
    """Send the Fink Flat Table

    Data is from /api/v1/ssoft

    Parameters
    ----------
    payload: dict
        See https://api.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    # Schema
    schema = payload.get("schema", False)
    if schema:
        if "flavor" in payload:
            flavor = payload["flavor"]
            if flavor not in ["SSHG1G2", "SHG1G2", "HG1G2", "HG"]:
                rep = {
                    "status": "error",
                    "text": "flavor needs to be in ['SSHG1G2', 'SHG1G2', 'HG1G2', 'HG']\n",
                }
                return Response(str(rep), 400)
            elif flavor == "SSHG1G2":
                ssoft_columns = {**COLUMNS, **COLUMNS_SSHG1G2}
            elif flavor == "SHG1G2":
                ssoft_columns = {**COLUMNS, **COLUMNS_SHG1G2}
            elif flavor == "HG1G2":
                ssoft_columns = {**COLUMNS, **COLUMNS_HG1G2}
            elif flavor == "HG":
                ssoft_columns = {**COLUMNS, **COLUMNS_HG}
        else:
            ssoft_columns = {**COLUMNS, **COLUMNS_SHG1G2}

        # return the schema of the table
        return Response(ssoft_columns, 200)

    # Table
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
        if version < "2023.07":
            rep = {
                "status": "error",
                "text": "version starts on 2023.07\n",
            }
            return Response(str(rep), 400)
    else:
        now = datetime.datetime.now()
        version = f"{now.year}.{now.month:02d}"

    if "flavor" in payload:
        flavor = payload["flavor"]
        if flavor not in ["SSHG1G2", "SHG1G2", "HG1G2", "HG"]:
            rep = {
                "status": "error",
                "text": "flavor needs to be in ['SSHG1G2', 'SHG1G2', 'HG1G2', 'HG']\n",
            }
            return Response(str(rep), 400)
    else:
        flavor = "SHG1G2"

    # Need to profile compared to pyarrow
    input_args = yaml.load(open("config.yml"), yaml.Loader)
    r = requests.get(
        "{}/SSOFT/ssoft_{}_{}.parquet?op=OPEN&user.name={}&namenoderpcaddress={}".format(
            input_args["WEBHDFS"],
            flavor,
            version,
            input_args["USER"],
            input_args["NAMENODE"],
        ),
    )

    if "sso_name" in payload:
        # TODO: use pyarrow instead
        pdf = pd.read_parquet(io.BytesIO(r.content))
        mask = pdf["sso_name"] == pdf["sso_name"]
        pdf = pdf[mask]
        pdf = pdf[pdf["sso_name"].astype("str") == payload["sso_name"]]
        return pdf
    elif "sso_number" in payload:
        # TODO: use pyarrow instead
        pdf = pd.read_parquet(io.BytesIO(r.content))
        mask = pdf["sso_number"] == pdf["sso_number"]
        pdf = pdf[mask]
        pdf = pdf[pdf["sso_number"].astype("int") == int(payload["sso_number"])]
        return pdf
    elif payload.get("output-format", "parquet") != "parquet":
        # Full table in other format than parquet (slow)
        return pd.read_parquet(io.BytesIO(r.content))
    else:
        # Full table in parquet (fast)
        return io.BytesIO(r.content)
