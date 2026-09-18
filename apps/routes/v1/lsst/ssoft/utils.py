# Copyright 2019-2026 AstroLab Software
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
import datetime
import io
import json

import pandas as pd
import requests
import yaml

# FIXME: update the columns wrt fink-science
from fink_utils.sso.ssoft import get_ssoft_columns
from flask import Response
from line_profiler import profile


def get_rid_nan_inf(pdf, col):
    """Remove problematic values"""
    is_decimal = pdf[col].apply(lambda x: str(x).isdecimal())
    return pdf[~pd.isna(pdf[col]) * is_decimal]


def get_schema(payload):
    """Wrapper to get columns given a flavor"""
    COLUMNS, COLUMNS_HG, COLUMNS_HG1G2, COLUMNS_SHG1G2, _ = get_ssoft_columns("lsst")
    if "flavor" in payload:
        flavor = payload["flavor"]
        if flavor not in ["SHG1G2", "HG1G2", "HG"]:
            rep = {
                "status": "error",
                "text": "flavor needs to be in ['SHG1G2', 'HG1G2', 'HG']\n",
            }
            return Response(str(rep), 400)
        elif flavor == "SHG1G2":
            ssoft_columns = {**COLUMNS, **COLUMNS_SHG1G2}
        elif flavor == "HG1G2":
            ssoft_columns = {**COLUMNS, **COLUMNS_HG1G2}
        elif flavor == "HG":
            ssoft_columns = {**COLUMNS, **COLUMNS_HG}
    else:
        ssoft_columns = {**COLUMNS, **COLUMNS_HG}

    return ssoft_columns


def get_user_columns(payload):
    columns = payload.get("columns", None)
    if columns is None:
        return None

    ssoft_columns = get_schema(payload)
    to_return = []
    [
        to_return.append(column)
        for column in columns.split(",")
        if column in ssoft_columns.keys()
    ]

    if to_return == []:
        return None
    return to_return


@profile
def get_ssoft(payload: dict) -> pd.DataFrame:
    """Send the Fink Flat Table

    Data is from /api/v1/ssoft

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
        ssoft_columns = get_schema(payload)
        if isinstance(ssoft_columns, Response):
            # Error propagation
            return ssoft_columns
        # return the schema of the table
        response = Response(json.dumps(ssoft_columns), 200)
        response.headers.set("Content-Type", "application/json")
        return response

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
        if version < "2026.08":
            rep = {
                "status": "error",
                "text": "version starts on 2026.08\n",
            }
            return Response(str(rep), 400)
    else:
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        version = f"{now.year}.{now.month:02d}"

    if "flavor" in payload:
        flavor = payload["flavor"]
        if flavor not in ["SHG1G2", "HG1G2", "HG"]:
            rep = {
                "status": "error",
                "text": "flavor needs to be in ['SHG1G2', 'HG1G2', 'HG']\n",
            }
            return Response(str(rep), 400)
    else:
        flavor = "HG"

    # Need to profile compared to pyarrow
    with open("config.yml") as f:
        input_args = yaml.load(f, yaml.Loader)
    r = requests.get(
        "{}/SSOFT/ssoft_{}_{}.parquet?op=OPEN&user.name={}&namenoderpcaddress={}".format(
            input_args["WEBHDFS"],
            flavor,
            version,
            input_args["USER"],
            input_args["NAMENODE"],
        ),
    )

    columns = get_user_columns(payload)
    if "sso_name" in payload:
        # TODO: use pyarrow instead
        pdf = pd.read_parquet(io.BytesIO(r.content), columns=columns)
        pdf = pdf[pdf["sso_name"].astype("str") == payload["sso_name"]]
        return pdf
    elif "sso_number" in payload:
        # TODO: use pyarrow instead
        pdf = pd.read_parquet(io.BytesIO(r.content), columns=columns)
        mask = pdf["sso_number"] == pdf["sso_number"]
        pdf = get_rid_nan_inf(pdf[mask], "sso_number")
        pdf = pdf[pdf["sso_number"].astype("int") == int(payload["sso_number"])]
        return pdf
    elif payload.get("output-format", "parquet") != "parquet":
        # Full table in other format than parquet (slow)
        return pd.read_parquet(io.BytesIO(r.content), columns=columns)
    else:
        if columns is not None:
            return pd.read_parquet(io.BytesIO(r.content), columns=columns)
        else:
            # Full table in parquet (fast)
            response = Response(io.BytesIO(r.content), 200)
            response.headers.set("Content-Type", "application/parquet")
            return response
