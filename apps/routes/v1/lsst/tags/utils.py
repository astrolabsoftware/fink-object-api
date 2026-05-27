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
import importlib
import pkgutil

import fink_filters.rubin.livestream as ffrl
import pandas as pd
from astropy.time import Time
from flask import Response
from line_profiler import profile

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_lsst_hbase_output


def extract_tags(with_description=False, hbase_support_only=False):
    """Extract user-defined tags

    Parameters
    ----------
    with_description: bool
        If True, extract descriptions, otherwise leave it empty.
        Default is False.
    hbase_support_only: bool
        If True, returns only filters with the HBase support.
        Default is False

    Returns
    -------
    tags: list of str
        List of tags
    descriptions: list of str
        Long descriptions for tags. Empty if with_description is False.
    """
    # User-defined topics
    userfilters = [
        "{}.{}.filter.{}".format(ffrl.__package__, mod, mod.split("filter_")[1])
        for _, mod, _ in pkgutil.iter_modules(ffrl.__path__)
    ]

    tags = [userfilter.split(".")[-1] for userfilter in userfilters]

    if hbase_support_only:
        # Get only filters with HBase support
        modules = [u.rsplit(".", maxsplit=1)[0] for u in userfilters]
        hbase_supports = [importlib.import_module(m).HBASE_SUPPORT for m in modules]
        userfilters = [
            u for u, hs in zip(userfilters, hbase_supports, strict=True) if hs
        ]

    if with_description:
        # Recompute modules as userfilters is changed inplace if hbase_support_only
        modules = [u.rsplit(".", maxsplit=1)[0] for u in userfilters]
        descriptions = [importlib.import_module(m).DESCRIPTION for m in modules]
    else:
        descriptions = ["" for u in userfilters]

    return tags, descriptions


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
    # Get the tag
    tag = payload["tag"]

    # Check the tag exists
    allowed_tags, _ = extract_tags(with_description=False, hbase_support_only=True)
    if tag not in allowed_tags:
        msg = f"""
        {tag} is not a valid tag. Here is the list of tags:
        {allowed_tags}
        And you can always retrieve available tags at https://api.lsst.fink-portal.org/api/v1/tags
        """
        return Response(msg, 400)

    nalerts = int(payload.get("n", 10))

    if "startdate" not in payload:
        # start of the Fink operations
        jd_start = Time("2019-11-01 00:00:00").mjd
    else:
        jd_start = Time(payload["startdate"]).mjd

    if "stopdate" not in payload:
        jd_stop = Time.now().mjd
    else:
        jd_stop = Time(payload["stopdate"]).mjd

    if "columns" in payload:
        cols = payload["columns"].replace(" ", "")
    else:
        cols = "*"

    if cols == "*":
        truncated = False
    else:
        truncated = True

    client = connect_to_hbase_table(f"rubin.tag_{tag}")

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
