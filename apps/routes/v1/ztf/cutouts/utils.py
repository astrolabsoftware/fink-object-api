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
from flask import send_file, jsonify, Response

import io
import json
import requests

import numpy as np
from matplotlib import cm
from PIL import Image

from apps.utils.client import connect_to_hbase_table
from apps.utils.plotting import sigmoid_normalizer, legacy_normalizer, convolve
from apps.utils.decoding import format_hbase_output
from apps.utils.utils import extract_configuration

from line_profiler import profile


@profile
def format_and_send_cutout(payload: dict):
    """Extract data returned by HBase and jsonify it

    Data is from /api/v1/cutouts

    Parameters
    ----------
    payload: dict
        See https://fink-portal.org/api/v1/cutouts

    Return
    ----------
    out: pandas dataframe
    """
    output_format = payload.get("output-format", "PNG")

    # default stretch is sigmoid
    if "stretch" in payload:
        stretch = payload["stretch"]
    else:
        stretch = "sigmoid"

    if payload["kind"] == "All" and payload["output-format"] != "array":
        rep = {
            "status": "error",
            "text": "The option `kind=All` is only compatible with `output-format=array`.\n",
        }
        return Response(str(rep), 400)

    # default name based on parameters
    filename = "{}_{}".format(
        payload["objectId"],
        payload["kind"],
    )

    if output_format == "PNG":
        filename = filename + ".png"
    elif output_format == "FITS":
        filename = filename + ".fits"
    elif output_format == "array":
        pass
    else:
        rep = {
            "status": "error",
            "text": "output-format must be one of: PNG, FITS, or array.\n",
        }
        return Response(str(rep), 400)

    # Query the Database (object query)
    client = connect_to_hbase_table("ztf.cutouts")
    results = client.scan(
        "",
        "key:key:{}".format(payload["objectId"]),
        "d:hdfs_path,i:jd,i:candid,i:objectId",
        0,
        False,
        False,
    )

    # Format the results
    schema_client = client.schema()
    client.close()

    pdf = format_hbase_output(
        results,
        schema_client,
        group_alerts=False,
        truncated=True,
        extract_color=False,
        escape_slash=False,
    )

    json_payload = {}
    # Extract only the alert of interest
    if "candid" in payload:
        mask = pdf["i:candid"].astype(str) == str(payload["candid"])
        json_payload.update({"candid": str(payload["candid"])})
        pos_target = np.where(mask)[0][0]
    else:
        # pdf has been sorted in `format_hbase_output`
        pdf = pdf.iloc[0:1]
        pos_target = 0

    json_payload.update(
        {
            "hdfsPath": pdf["d:hdfs_path"].to_numpy()[pos_target].split(":8020")[1],
            "kind": payload["kind"],
            "objectId": pdf["i:objectId"].to_numpy()[pos_target],
        }
    )

    if pdf.empty:
        return send_file(
            io.BytesIO(),
            mimetype="image/png",
            as_attachment=True,
            download_name=filename,
        )

    # Extract cutouts
    user_config = extract_configuration("config.yml")
    cutout = request_cutout(json_payload, output_format, user_config["CUTOUTAPIURL"])

    # send the FITS file
    if output_format == "FITS":
        return send_file(
            cutout,
            mimetype="application/octet-stream",
            as_attachment=True,
            download_name=filename,
        )
    # send the array
    elif output_format == "array":
        if payload["kind"] != "All":
            return jsonify({"b:cutout{}_stampData".format(payload["kind"]): cutout[0]})
        else:
            out = {
                "b:cutoutScience_stampData": cutout[0],
                "b:cutoutTemplate_stampData": cutout[1],
                "b:cutoutDifference_stampData": cutout[2],
            }
            return jsonify(out)

    array = np.nan_to_num(np.array(cutout[0], dtype=float))
    if stretch == "sigmoid":
        array = sigmoid_normalizer(array, 0, 1)
    elif stretch is not None:
        pmin = 0.5
        if "pmin" in payload:
            pmin = float(payload["pmin"])
        pmax = 99.5
        if "pmax" in payload:
            pmax = float(payload["pmax"])
        array = legacy_normalizer(array, stretch=stretch, pmin=pmin, pmax=pmax)

    if "convolution_kernel" in payload:
        assert payload["convolution_kernel"] in ["gauss", "box"]
        array = convolve(array, smooth=1, kernel=payload["convolution_kernel"])

    # colormap
    if "colormap" in payload:
        colormap = getattr(cm, payload["colormap"])
    else:
        colormap = lambda x: x  # noqa: E731
    array = np.uint8(colormap(array) * 255)

    # Convert to PNG
    data = Image.fromarray(array)
    datab = io.BytesIO()
    data.save(datab, format="PNG")
    datab.seek(0)
    return send_file(
        datab, mimetype="image/png", as_attachment=True, download_name=filename
    )


@profile
def request_cutout(json_payload, output_format, cutout_api_url):
    """Request a cutout from the Fink cutout API

    Parameters
    ----------
    json_payload: dict
        Dictionary with arguments for /api/v1/cutouts
    output_format: str
        Among: FITS, PNG, array
    cutout_api_url: str
        URL of the Fink cutout API service.

    Returns
    -------
    cutout: Any
        Output type depends on the `output_format` argument
    """
    if output_format == "FITS":
        json_payload.update({"return_type": "FITS"})
        r0 = requests.post(
            "{}/api/v1/cutouts".format(cutout_api_url), json=json_payload
        )
        cutout = io.BytesIO(r0.content)
    elif output_format in ["PNG", "array"]:
        json_payload.update({"return_type": "array"})
        r0 = requests.post(
            "{}/api/v1/cutouts".format(cutout_api_url), json=json_payload
        )
        cutout = json.loads(r0.content)
    return cutout
