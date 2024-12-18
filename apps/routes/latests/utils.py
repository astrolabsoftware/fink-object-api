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
from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_hbase_output

from line_profiler import profile


@profile
def extract_object_from_class(payload: dict, return_raw: bool = False) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/latests

    Parameters
    ----------
    payload: dict
        See https://api.fink-portal.org
    return_raw: bool
        If True, return the HBase output, else pandas DataFrame. Default is False.

    Return
    ----------
    out: pandas dataframe
    """
    if "n" not in payload:
        nalerts = 10
    else:
        nalerts = int(payload["n"])

    if "startdate" not in payload:
        # start of the Fink operations
        jd_start = Time("2019-11-01 00:00:00").jd
    else:
        jd_start = Time(payload["startdate"]).jd

    if "stopdate" not in payload:
        jd_stop = Time.now().jd
    else:
        jd_stop = Time(payload["stopdate"]).jd

    if "color" not in payload:
        color = False
    else:
        color = True

    if "columns" in payload:
        cols = payload["columns"].replace(" ", "")
    else:
        cols = "*"

    if cols == "*":
        truncated = False
    else:
        truncated = True

    # Search for latest alerts for a specific class
    tns_classes = pd.read_csv("assets/tns_types.csv", header=None)[0].to_numpy()
    is_tns = payload["class"].startswith("(TNS)") and (
        payload["class"].split("(TNS) ")[1] in tns_classes
    )
    if is_tns:
        client = connect_to_hbase_table("ztf.tns")
        classname = payload["class"].split("(TNS) ")[1]
        client.setLimit(nalerts)
        client.setRangeScan(True)
        client.setReversed(True)

        results = client.scan(
            "",
            f"key:key:{classname}_{jd_start},key:key:{classname}_{jd_stop}",
            cols,
            0,
            True,
            True,
        )
        schema_client = client.schema()
        group_alerts = True
    elif payload["class"].startswith("(SIMBAD)") or payload["class"] != "allclasses":
        if payload["class"].startswith("(SIMBAD)"):
            classname = payload["class"].split("(SIMBAD) ")[1]
        else:
            classname = payload["class"]

        client = connect_to_hbase_table("ztf.class")

        client.setLimit(nalerts)
        client.setRangeScan(True)
        client.setReversed(True)

        results = client.scan(
            "",
            f"key:key:{classname}_{jd_start},key:key:{classname}_{jd_stop}",
            cols,
            0,
            False,
            False,
        )
        schema_client = client.schema()
        group_alerts = False
    elif payload["class"] == "allclasses":
        client = connect_to_hbase_table("ztf.jd")
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
        group_alerts = False

    client.close()

    if return_raw:
        return results

    # We want to return alerts
    # color computation is disabled
    pdf = format_hbase_output(
        results,
        schema_client,
        group_alerts=group_alerts,
        extract_color=color,
        truncated=truncated,
        with_constellation=True,
    )

    return pdf

