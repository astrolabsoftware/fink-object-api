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
import io
import gzip
import requests

from astropy.io import fits
from astropy.time import Time

import healpy as hp
import pandas as pd
import numpy as np

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_hbase_output

from line_profiler import profile


@profile
def search_in_skymap(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and jsonify it

    Data is from /api/v1/bayestar

    Parameters
    ----------
    payload: dict
        See https://api.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    # boundaries in day
    n_day_before = payload.get("n_day_before", 1)
    n_day_after = payload.get("n_day_after", 6)

    # Interpret user input
    if "bayestar" in payload:
        bayestar_data = payload["bayestar"]
    elif "event_name" in payload:
        r = requests.get(
            "https://gracedb.ligo.org/api/superevents/{}/files/bayestar.fits.gz".format(
                payload["event_name"]
            )
        )
        if r.status_code == 200:
            bayestar_data = str(r.content)
        else:
            return pd.DataFrame([{"status": r.content}])
    credible_level_threshold = float(payload["credible_level"])

    with gzip.open(io.BytesIO(eval(bayestar_data)), "rb") as f:
        with fits.open(io.BytesIO(f.read())) as hdul:
            data = hdul[1].data
            header = hdul[1].header

    hpx = data["PROB"]
    if header["ORDERING"] == "NESTED":
        hpx = hp.reorder(hpx, n2r=True)

    i = np.flipud(np.argsort(hpx))
    sorted_credible_levels = np.cumsum(hpx[i])
    credible_levels = np.empty_like(sorted_credible_levels)
    credible_levels[i] = sorted_credible_levels

    # TODO: use that to define the max skyfrac (in conjunction with level)
    # npix = len(hpx)
    # nside = hp.npix2nside(npix)
    # skyfrac = np.sum(credible_levels <= 0.1) * hp.nside2pixarea(nside, degrees=True)

    credible_levels_128 = hp.ud_grade(credible_levels, 128)

    pixs = np.where(credible_levels_128 <= credible_level_threshold)[0]

    # make a condition as well on the number of pixels?
    # print(len(pixs), pixs)

    # For the future: we could set clientP128.setRangeScan(True)
    # and pass directly the time boundaries here instead of
    # grouping by later.

    # 1 day before the event, to 6 days after the event
    mjdstart = Time(header["DATE-OBS"], scale="utc").tai.mjd - n_day_before
    mjdend = mjdstart + n_day_after

    # FIXME: filtering on time does not work as 
    # r:firstDiaSourceMjdTai is not populated yet
    client = connect_to_hbase_table("rubin.pixel128")
    # client.setRangeScan(True)
    results = {}
    for pix in pixs:
        # to_search = f"key:key:{pix}_{mjdstart},key:key:{pix}_{mjdend}"
        to_search = f"key:key:{pix}_"
        result = client.scan(
            "",
            to_search,
            "*",
            0,
            True,
            True,
        )
        results.update(result)

    schema_client = client.schema()
    client.close()

    pdf = format_hbase_output(
        results,
        schema_client,
        truncated=True,
        group_alerts=True,
        extract_color=False,
    )

    if pdf.empty:
        return pdf

    pdf["v:startgwMjdTai"] = Time(header["DATE-OBS"], scale="utc").tai.mjd

    # FIXME: should make se of r:firstDiaSourceMjdTai when it will be
    # populated by the project...
    mask = (pdf["r:midpointMjdTai"] - pdf["r:midpointMjdTai"]) <= n_day_after

    return pdf[mask]
