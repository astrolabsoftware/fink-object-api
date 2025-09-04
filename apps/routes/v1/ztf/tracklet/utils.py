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

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_hbase_output

from line_profiler import profile


@profile
def get_tracklet(payload: dict):
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/tracklet

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

    if "id" in payload:
        payload_name = payload["id"]
    elif "date" in payload:
        designation = payload["date"]
        payload_name = "TRCK_" + designation.replace("-", "").replace(":", "").replace(
            " ", "_"
        )
    else:
        rep = {
            "status": "error",
            "text": "You need to specify a date at the format YYYY-MM-DD hh:mm:ss\n",
        }
        return Response(str(rep), 400)

    # Note the trailing _
    to_evaluate = f"key:key:{payload_name}"

    client = connect_to_hbase_table("ztf.tracklet")
    results = client.scan(
        "",
        to_evaluate,
        cols,
        0,
        True,
        True,
    )

    schema_client = client.schema()

    # reset the limit in case it has been changed above
    client.close()

    pdf = format_hbase_output(
        results,
        schema_client,
        group_alerts=False,
        truncated=truncated,
        extract_color=False,
    )

    return pdf
