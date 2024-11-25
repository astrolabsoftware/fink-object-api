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
from apps.utils import send_tabular_data

from apps.routes.objects.utils import extract_object_data

bp = Blueprint("objects", __name__)

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
        "description": 'single ZTF Object ID, or a comma-separated list of object names, e.g. "ZTF19acmdpyr,ZTF21aaxtctv"',
    },
    {
        "name": "withupperlim",
        "required": False,
        "description": "If True, retrieve also upper limit measurements, and bad quality measurements. Use the column `d:tag` in your results: valid, upperlim, badquality.",
    },
    {
        "name": "withcutouts",
        "required": False,
        "description": "If True, retrieve also cutout data as 2D array. See also `cutout-kind`. More information on the original cutouts at https://irsa.ipac.caltech.edu/data/ZTF/docs/ztf_explanatory_supplement.pdf",
    },
    {
        "name": "cutout-kind",
        "required": False,
        "description": "`Science`, `Template`, or `Difference`. If not specified, returned all three.",
    },
    {
        "name": "columns",
        "required": False,
        "description": f"Comma-separated data columns to transfer. Default is all columns. See {APIURL}/api/v1/columns for more information.",
    },
    {
        "name": "output-format",
        "required": False,
        "description": "Output format among json[default], csv, parquet, votable",
    },
]

@bp.route("/api/v1/objects", methods=["GET"])
def return_object_arguments():
    """Obtain information about retrieving object data"""
    if len(request.args) > 0:
        # POST from query URL
        return return_object(payload=request.args)
    else:
        return jsonify({"args": ARGS})


@bp.route("/api/v1/objects", methods=["POST"])
def return_object(payload=None):
    """Retrieve object data from the Fink database"""
    # get payload from the JSON
    if payload is None:
        payload = request.json

    rep = check_args(ARGS, payload)
    if rep["status"] != "ok":
        return Response(str(rep), 400)

    out = extract_object_data(payload)

    # Error propagation
    if isinstance(out, Response):
        return out

    output_format = payload.get("output-format", "json")
    return send_tabular_data(out, output_format)
