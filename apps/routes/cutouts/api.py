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
from flask import Blueprint, Response, jsonify, request
from apps.utils import check_args

from apps.routes.cutouts.utils import format_and_send_cutout

bp = Blueprint("cutouts", __name__)


# Enable CORS for this blueprint
@bp.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
    return response


ARGS = [
    {
        "name": "objectId",
        "required": True,
        "description": "ZTF Object ID",
    },
    {
        "name": "kind",
        "required": True,
        "description": "Science, Template, or Difference. For output-format=array, you can also specify `kind: All` to get the 3 cutouts.",
    },
    {
        "name": "output-format",
        "required": False,
        "description": "PNG[default], FITS, array",
    },
    {
        "name": "candid",
        "required": False,
        "description": "Candidate ID of the alert belonging to the object with `objectId`. If not filled, the cutouts of the latest alert is returned",
    },
    {
        "name": "stretch",
        "required": False,
        "description": "Stretch function to be applied. Available: sigmoid[default], linear, sqrt, power, log.",
    },
    {
        "name": "colormap",
        "required": False,
        "description": "Valid matplotlib colormap name (see matplotlib.cm). Default is grayscale.",
    },
    {
        "name": "pmin",
        "required": False,
        "description": "The percentile value used to determine the pixel value of minimum cut level. Default is 0.5. No effect for sigmoid.",
    },
    {
        "name": "pmax",
        "required": False,
        "description": "The percentile value used to determine the pixel value of maximum cut level. Default is 99.5. No effect for sigmoid.",
    },
    {
        "name": "convolution_kernel",
        "required": False,
        "description": "Convolve the image with a kernel (gauss or box). Default is None (not specified).",
    },
]


@bp.route("/api/v1/cutouts", methods=["GET"])
def return_cutouts_arguments():
    """Obtain information about cutouts"""
    if len(request.args) > 0:
        # POST from query URL
        return return_cutouts(payload=request.args)
    else:
        return jsonify({"args": ARGS})


@bp.route("/api/v1/cutouts", methods=["POST"])
def return_cutouts(payload=None):
    """Retrieve object data"""
    # get payload from the JSON
    if payload is None:
        payload = request.json

    rep = check_args(ARGS, payload)
    if rep["status"] != "ok":
        return Response(str(rep), 400)

    assert payload["kind"] in ["Science", "Template", "Difference", "All"]

    return format_and_send_cutout(payload)
