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
from flask import Response

import io
import yaml
import requests

import pandas as pd

from line_profiler import profile


@profile
def get_lc(payload: dict) -> pd.DataFrame:
    """Send the Fink Flat Table

    Data is from /api/v1/ssobulk

    Parameters
    ----------
    payload: dict
        See https://api.ztf.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    # Need to profile compared to pyarrow
    input_args = yaml.load(open("config.yml"), yaml.Loader)
    r = requests.get(
        "{}/sso_ztf_lc_aggregated_with_ssoft_202601_with_residuals.parquet?op=OPEN&user.name={}&namenoderpcaddress={}".format(
            input_args["WEBHDFS"],
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
