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
from flask import Response

import io
import requests

import pandas as pd
from astropy.time import Time

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_hbase_output
from apps.utils.utils import extract_configuration

from line_profiler import profile


@profile
def extract_trendy_data(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/trend

    Parameters
    ----------
    payload: dict
        See https://api.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    if payload["trend"] not in ["rising", "fading", "low_state"]:
        msg = """
        {} is not a valid trend.
        Trend must be among: rising, fading, low_state
        """.format(payload["trend"])
        return Response(msg, 400)

    if payload["trend"] == "low_state" and payload["class"] != "CTAOBlazar":
        msg = """
        low_state trend is only implemented for the CTAOBlazar class.
        {} class can accept trend among: rising, fading.
        """.format(payload["class"])
        return Response(msg, 400)

    if "columns" in payload:
        cols = payload["columns"].replace(" ", "")
        if not "d:mag_rate" in cols:
            cols += ",d:mag_rate"
        truncated = True
    else:
        cols = "*"
        truncated = False

    if "n" not in payload:
        nalerts = 100
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

    if payload["class"] == "CTAOBlazar":
        # CTAO Blazars with low states
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

        pdf = format_hbase_output(
            results,
            client.schema(),
            group_alerts=True,
            extract_color=False,
            truncated=truncated,
            with_constellation=False,
        )
        client.close()
    else:
        # Any other known class
        # rest api + filter on mag_rate?
        # what if 0 objects?
        config = extract_configuration("config.yml")

        r = requests.post(
            "{}/api/v1/latests".format(config["APIURL"]),
            json={
                "class": payload["class"],
                "n": payload["n"],
                "columns": cols,
                "startdate": payload.get("startdate", "2019-11-01 00:00:00"),
                "stopdate": payload.get("stopdate", Time.now().iso),
            },
        )

        if r.status_code != 200:
            return Response(str(r.text), 400)

        # Format output in a DataFrame
        pdf = pd.read_json(io.BytesIO(r.content))

        # Search for trend
        if payload["trend"] == "rising":
            pdf = pdf[pdf["d:mag_rate"] < 0]
        elif payload["trend"] == "fading":
            pdf = pdf[pdf["d:mag_rate"] > 0]
        else:
            msg = "Trend must be among: rising, fading"
            return Response(msg, 400)

    return pdf
