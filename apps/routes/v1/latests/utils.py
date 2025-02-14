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

import numpy as np
import pandas as pd
from astropy.time import Time

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_hbase_output

from line_profiler import profile


def extract_feature(string, pos):
    """ """
    mylist = [
        float(num.strip()) if num.strip() != "NaN" else np.nan
        for num in string.strip("{}").split(",")
    ]

    if isinstance(mylist, list):
        if len(mylist) > pos:
            return mylist[pos]
    return np.nan

def add_trend(row, pos):
    """
    """
    if row['i:fid'] == 1:
        return extract_feature(row["d:lc_features_g"], pos)
    else:
        return extract_feature(row["d:lc_features_r"], pos)


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
    if "trend" in payload and payload["trend"] not in [
        "rising",
        "fading",
        "low_state",
        "new_low_state",
    ]:
        msg = """
        {} is not a valid trend.
        Trend must be among: rising, fading, low_state, new_low_state
        """.format(payload["trend"])
        return Response(msg, 400)

    if (
        payload.get("trend", None) in ["low_state", "new_low_state"]
        and payload.get("class", None) != "(CTA) Blazar"
    ):
        msg = """
        {} trend is only implemented for the `(CTA) Blazar` class.
        {} class can accept trend among: rising, fading.
        """.format(payload["trend"], payload["class"])
        return Response(msg, 400)

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
    is_cta_blazar = payload["class"] == "(CTA) Blazar"
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
    elif is_cta_blazar:
        # CTAO Blazars with low states
        # To be changed when more trend will appear, like flares
        client = connect_to_hbase_table("ztf.low_state_blazars")

        client.setLimit(nalerts)
        client.setRangeScan(True)
        client.setReversed(True)

        results = client.scan(
            "",
            f"key:key:{jd_start}_,key:key:{jd_stop}_",
            cols,
            0,
            False,
            False,
        )
        schema_client = client.schema()
        group_alerts = True
    else:
        if payload["class"].startswith("(SIMBAD)"):
            # SIMBAD crossmatch
            classname = payload["class"].split("(SIMBAD) ")[1]
        else:
            # Fink classification
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

    # Search for trend
    # TODO: add fink-filters here!
    if payload.get("trend", None) == "rising":
        f0 = (pdf["d:mag_rate"] < 0) # & ~pdf["d:from_upper"]
        if "d:lc_features_g" in pdf.columns:
            # pos=9 is linear trend
            f1 = pdf.apply(lambda row: add_trend(row, 9), axis=1) < 0
            pdf = pdf[f0 & f1]
        else:
            pdf = pdf[f0]
    elif payload.get("trend", None) == "fading":
        f0 = (pdf["d:mag_rate"] > 0) # & ~pdf["d:from_upper"]
        if "d:lc_features_g" in pdf.columns:
            # pos=9 is linear trend
            f1 = pdf.apply(lambda row: add_trend(row, 9), axis=1) > 0
            pdf = pdf[f0 & f1]
        else:
            pdf = pdf[f0]
    elif payload.get("trend", None) == "new_low_state":
        # TODO: use fink-filters directly
        # TODO: return exception if field not present
        pdf = pdf[pdf["d:blazar_stats_m0"] >= 1]

    return pdf
