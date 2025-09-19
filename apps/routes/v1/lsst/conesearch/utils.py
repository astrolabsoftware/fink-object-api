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

import pandas as pd

from numpy import pi as nppi
from healpy import query_disc, ang2vec

import astropy.units as u
from astropy.time import Time
from astropy.coordinates import SkyCoord

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_lsst_hbase_output
from apps.utils.utils import isoify_time

from line_profiler import profile


@profile
def run_conesearch(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/conesearch

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
        if "s:ra" not in cols:
            cols = ",".join([cols, "s:ra"])
        if "s:dec" not in cols:
            cols = ",".join([cols, "s:dec"])
    else:
        cols = "*"

    n = int(payload.get("n", 1000))

    # Conesearch with optional date range
    client = connect_to_hbase_table("rubin.pixel128")
    client.setLimit(n)

    # Interpret user input
    ra, dec = payload["ra"], payload["dec"]
    radius = payload["radius"]

    if float(radius) > 18000.0:
        rep = {
            "status": "error",
            "text": "`radius` cannot be bigger than 18,000 arcseconds (5 degrees).\n",
        }
        return Response(str(rep), 400)

    try:
        if "h" in str(ra):
            coord = SkyCoord(ra, dec, frame="icrs")
        elif ":" in str(ra) or " " in str(ra):
            coord = SkyCoord(ra, dec, frame="icrs", unit=(u.hourangle, u.deg))
        else:
            coord = SkyCoord(ra, dec, frame="icrs", unit="deg")
    except ValueError as e:
        rep = {
            "status": "error",
            "text": e,
        }
        return Response(str(rep), 400)

    ra = coord.ra.deg
    dec = coord.dec.deg
    radius_deg = float(radius) / 3600.0

    # angle to vec conversion
    vec = ang2vec(nppi / 2.0 - nppi / 180.0 * dec, nppi / 180.0 * ra)

    # Send request
    nside = 128

    pixs = query_disc(
        nside,
        vec,
        nppi / 180 * radius_deg,
        inclusive=True,
    )

    results = {}
    for pix in pixs:
        to_search = f"key:key:{pix}_"
        result = client.scan(
            "",
            to_search,
            cols,
            0,
            True,
            True,
        )
        results.update(result)

    schema_client = client.schema()

    client.close()

    pdf = format_lsst_hbase_output(
        results,
        schema_client,
        truncated=True,
        group_alerts=True,
        extract_color=False,
    )

    # Filter by time
    # FIXME: does not work yet as firstDiaSourceMjdTai is not populated
    if "startdate" in payload:
        # Filter out alerts that vary in the past
        mjd_start = Time(isoify_time(payload["startdate"]), scale="tai").mjd
        pdf = pdf[pdf["o:firstDiaSourceMjdTai"] >= mjd_start]

        if "window" in payload:
            # Also filter out alerts that vary in the future
            window = float(payload["window"])
            mjd_stop = mjd_start + window
            pdf = pdf[pdf["o:lastDiaSourceMjdTai"] <= mjd_stop]

    if "stopdate" in payload:
        # Filter out alerts that vary in the future
        mjd_stop = Time(isoify_time(payload["stopdate"]), scale="tai").mjd
        pdf = pdf[pdf["o:lastDiaSourceMjdTai"] <= mjd_stop]

    # For conesearch, sort by distance
    if len(pdf) > 0:
        sep = coord.separation(
            SkyCoord(
                pdf["s:ra"],
                pdf["s:dec"],
                unit="deg",
            ),
        ).deg

        pdf["v:separation_degree"] = sep
        pdf = pdf.sort_values("v:separation_degree", ascending=True)

        mask = pdf["v:separation_degree"] > radius_deg
        pdf = pdf[~mask]

    return pdf
