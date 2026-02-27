# Copyright 2025-2026 AstroLab Software
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

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_lsst_hbase_output

from line_profiler import profile


@profile
def extract_object_data(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/objects

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

    if "," in payload["diaObjectId"]:
        # multi-objects search
        splitids = payload["diaObjectId"].split(",")
        splitids = [i.strip() for i in splitids]
        # add salt
        objectids = [f"key:key:{i[-3:]}_{i}" for i in splitids]
    else:
        # single object search
        objectids = [
            "key:key:{}_{}".format(payload["diaObjectId"][-3:], payload["diaObjectId"])
        ]

    if cols == "*":
        truncated = False
    else:
        truncated = True

    client = connect_to_hbase_table("rubin.diaObject")

    # Get data from the main table
    results = {}
    for to_evaluate in objectids:
        result = client.scan(
            "",
            to_evaluate,
            cols,
            0,
            True,
            True,
        )
        results.update(result)

    schema_client = client.schema()

    pdf = format_lsst_hbase_output(
        results,
        schema_client,
        group_alerts=False,
        truncated=truncated,
    )

    client.close()

    return pdf
