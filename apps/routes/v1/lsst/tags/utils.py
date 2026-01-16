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

import pandas as pd
import pkgutil

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_lsst_hbase_output

import fink_filters.rubin.livestream as ffrl

from line_profiler import profile
from astropy.time import Time


def extract_tags():
    """Extract user-defined tags

    Returns
    -------
    out: list of str
        List of tags
    """
    # User-defined topics
    userfilters = [
        "{}.{}.filter.{}".format(ffrl.__package__, mod, mod.split("filter_")[1])
        for _, mod, _ in pkgutil.iter_modules(ffrl.__path__)
    ]
    tags = [userfilter.split(".")[-1] for userfilter in userfilters]

    return tags


@profile
def extract_object_data(payload: dict, return_raw: bool = False) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/tags

    Parameters
    ----------
    payload: dict
        See https://api.lsst.fink-portal.org
    return_raw: bool
        If True, return the HBase output, else pandas DataFrame.
        Default is False.

    Return
    ----------
    out: pandas dataframe
    """
    # Get the class
    tag = payload["tag"]

    # Check the tag exists
    if tag not in extract_tags():
        msg = """
        {} is not a valid tag. Here is the list of tags:
        {}
        And you can always retrieve available tags at https://api.lsst.fink-portal.org/api/v1/tags
        """.format(tag, extract_tags())
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

    if "columns" in payload:
        cols = payload["columns"].replace(" ", "")
    else:
        cols = "*"

    if cols == "*":
        truncated = False
    else:
        truncated = True

    client = connect_to_hbase_table("rubin.{}".format(tag))

    client.setLimit(nalerts)
    client.setRangeScan(True)
    client.setReversed(True)

    results = client.scan(
        "",
        f"key:key:{jd_start},key:key:{jd_stop}",
        cols,
        0,
        False,
        False,
    )
    schema_client = client.schema()

    client.close()

    if return_raw:
        return results

    pdf = format_lsst_hbase_output(
        results,
        schema_client,
        group_alerts=False,
        truncated=truncated,
    )

    return pdf
