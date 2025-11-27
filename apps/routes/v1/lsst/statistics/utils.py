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

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import hbase_to_dict

from line_profiler import profile


@profile
def get_statistics(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and jsonify it

    Data is from /api/v1/statistics

    Parameters
    ----------
    payload: dict
        See https://api.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    if "columns" in payload:
        cols = payload["columns"]
    else:
        cols = "*"

    client = connect_to_hbase_table("statistics_class")
    if "schema" in payload and str(payload["schema"]) == "True":
        # TODO: change the strategy to get the schema
        # The table schema changes everyday, so it is not very useful
        # Schema should use 3 things: /classes, basic:, and date
        schema = client.schema()
        results = list(schema.columnNames())
        pdf = pd.DataFrame({"schema": results})
    else:
        payload_date = payload["date"]

        to_evaluate = f"key:key:{payload_date}"
        results = client.scan(
            "",
            to_evaluate,
            cols,
            0,
            True,
            True,
        )
        pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")

        # See https://github.com/astrolabsoftware/fink-science-portal/issues/579
        pdf = pdf.replace(regex={r"^\x00.*$": 0})

    client.close()

    return pdf
