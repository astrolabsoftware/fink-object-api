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
import os
import io
import polars as pl
import datetime
import json
import requests
import yaml
from flask import Response
from line_profiler import profile
import pyarrow as pa

SSOBULKFILE = "sso_ztf_lc_aggregated_{}.parquet"

def generate(filename):
    parquet_file = pq.ParquetFile(filename)

    # Stream record batches (Arrow's native chunk format)
    for i in range(parquet_file.num_row_groups):
        batch = parquet_file.read_row_group(i)
        sink = io.BytesIO()
        writer = pa.ipc.new_stream(sink, batch.schema)
        writer.write_table(batch)
        writer.close()
        yield sink.getvalue()


@profile
def get_lc(payload: dict) -> pl.DataFrame:
    """Send the Fink Flat Table

    Data is from /api/v1/ssobulk

    Parameters
    ----------
    payload: dict
        See https://api.ztf.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    # Schema
    schema = payload.get("schema", False)
    if schema:
        SCHEMA = {
            "ssnamenr": {
                "type": "str",
                "description": "Official name or provisional designation of the SSO",
            },
            "cra": {"type": "list", "description": "List of RA in degree"},
            "cdec": {"type": "list", "description": "List of DEC in degree"},
            "cmagpsf": {"type": "list", "description": "List of difference magnitudes"},
            "csigmapsf": {
                "type": "list",
                "description": "List of difference magnitude error estimates",
            },
            "cfid": {
                "type": "list",
                "description": "List of filter band ID: 1 = g, 2 = r",
            },
            "cjd": {
                "type": "list",
                "description": "List of observing times in JD (UTC)",
            },
            "Dobs": {
                "type": "list",
                "description": "List of topocentric distances in AU",
            },
            "Dhelio": {
                "type": "list",
                "description": "List of heliocentric distances in AU",
            },
            "Phase": {
                "type": "list",
                "description": "List of phase angles in degree",
            },
            "Elong": {
                "type": "list",
                "description": "List of elongation angles in degree",
            },
            "RA": {
                "type": "list",
                "description": "List of RA ephemerides in degree",
            },
            "DEC": {
                "type": "list",
                "description": "List of DEC ephemerides in degree",
            },
            "RA_h": {
                "type": "list",
                "description": "List of Sun RA ephemerides in degree",
            },
            "DEC_h": {
                "type": "list",
                "description": "List of Sun DEC ephemerides in degree",
            },
            "px_ec": {
                "type": "list",
                "description": "List of x-coordinates of asteroids in topocentric in AU",
            },
            "py_ec": {
                "type": "list",
                "description": "List of y-coordinates of asteroids in topocentric in AU",
            },
            "pz_ec": {
                "type": "list",
                "description": "List of z-coordinates of asteroids in topocentric in AU",
            },
            "px_h_ec": {
                "type": "list",
                "description": "List of x-coordinates of asteroids in heliocentric in AU",
            },
            "py_h_ec": {
                "type": "list",
                "description": "List of y-coordinates of asteroids in heliocentric in AU",
            },
            "pz_h_ec": {
                "type": "list",
                "description": "List of z-coordinates of asteroids in heliocentric in AU",
            },
        }
        # return the schema of the table
        response = Response(json.dumps(SCHEMA), 200)
        response.headers.set("Content-Type", "application/json")
        return response

    # Need to profile compared to pyarrow
    with open("config.yml") as f:
        input_args = yaml.load(f, yaml.Loader)

    if "version" in payload:
        version = payload["version"]

        # version needs YYYY.MM
        yyyymm = version.split(".")
        if (len(yyyymm[0]) != 4) or (len(yyyymm[1]) != 2):
            rep = {
                "status": "error",
                "text": "version needs to be YYYY.MM\n",
            }
            return Response(str(rep), 400)
        if version < "2026.09":
            rep = {
                "status": "error",
                "text": "version starts on 2026.09\n",
            }
            return Response(str(rep), 400)
    else:
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        version = f"{now.year}.{now.month:02d}"

    cache_file = os.path.join("/scratch", SSOBULKFILE.format(version))
    if os.path.exists(cache_file):
        # Read existing file
        if "sso_name" in payload:
            pdf = pl.read_parquet(cache_file)
            matching = pdf.filter(
                pl.col("ssnamenr").cast(pl.String) == payload["sso_name"]
            )

            if matching.height > 0:
                return matching
            else:
                return pl.DataFrame()
        else:
            return Response(generate(cache_file), mimetype='application/octet-stream')
    else:
        # Download entire file
        # Get file list
        r = requests.get(
            "{}/SSOBULK/{}?op=LISTSTATUS&user.name={}&namenoderpcaddress={}".format(
                input_args["WEBHDFS"],
                SSOBULKFILE.format(version),
                input_args["USER"],
                input_args["NAMENODE"],
            ),
        )

        if r.status_code != 200:
            response = Response(r.text, r.status_code)
            return response

        frames = []
        for dic in r.json()["FileStatuses"]["FileStatus"]:
            filename = dic["pathSuffix"]
            if filename.endswith(".parquet"):
                r0 = requests.get(
                    "{}/SSOBULK/{}/{}?op=OPEN&user.name={}&namenoderpcaddress={}".format(
                        input_args["WEBHDFS"],
                        SSOBULKFILE.format(version),
                        filename,
                        input_args["USER"],
                        input_args["NAMENODE"],
                    ),
                )
                sub = pl.read_parquet(io.BytesIO(r0.content))
                if "sso_name" in payload:
                    matching = sub.filter(
                        pl.col("ssnamenr").cast(pl.String) == payload["sso_name"]
                    )

                    if matching.height > 0:
                        return matching
                else:
                    frames.append(sub)

        if frames:
            pdf = pl.concat(frames)

            # Save on disk for future queries
            pdf.write_parquet(cache_file)
            return pdf
        else:
            return pl.DataFrame()
