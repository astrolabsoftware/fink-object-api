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
import pandas as pd

from astropy.time import Time

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_hbase_output

from line_profiler import profile


@profile
def get_anomalous_alerts(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/anomaly

    Parameters
    ----------
    payload: dict
        See https://api.ztf.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    if "n" not in payload:
        nalerts = 10
    else:
        nalerts = int(payload["n"])

    if "start_date" not in payload:
        # start of the Fink operations
        jd_start = Time("2019-11-01 00:00:00").jd
    else:
        jd_start = Time(payload["start_date"]).jd

    if "stop_date" not in payload:
        jd_stop = Time.now().jd
    else:
        # allow to get unique day
        jd_stop = Time(payload["stop_date"]).jd + 1

    if "columns" in payload:
        cols = payload["columns"].replace(" ", "")
    else:
        cols = "*"

    if cols == "*":
        truncated = False
    else:
        truncated = True

    client = connect_to_hbase_table("ztf.anomaly")
    client.setLimit(nalerts)
    client.setRangeScan(True)
    client.setReversed(True)

    to_evaluate = f"key:key:{jd_start},key:key:{jd_stop}"
    results = client.scan(
        "",
        to_evaluate,
        cols,
        0,
        True,
        True,
    )
    schema_client = client.schema()
    client.close()

    # We want to return alerts
    # color computation is disabled
    pdf = format_hbase_output(
        results,
        schema_client,
        group_alerts=False,
        extract_color=False,
        truncated=truncated,
        with_constellation=True,
    )

    return pdf
