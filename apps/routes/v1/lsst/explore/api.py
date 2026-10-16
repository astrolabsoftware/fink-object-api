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
from flask import Response, jsonify, request
from flask_restx import Namespace, Resource, fields

from apps.routes.v1.lsst.explore.utils import extract_object_data, extract_classes
from apps.utils.utils import check_args, send_tabular_data

ns = Namespace("api/v1/explore", "Get samples of Rubin catalogued alerts")

ARGS = ns.model(
    "explore",
    {
        "class": fields.String(
            description="Class name in the form CATALOG/CLASSNAME. See https://api.lsst.fink-portal.org/api/v1/explore for available classes",
            example="TNS/SN Ia",
            required=True,
        ),
        "columns": fields.String(
            description="Comma-separated data columns to transfer, e.g. 'r:psfFlux,r:midpointMjdTai,r:diaObjectId'. If not specified, transfer all columns.",
            example="r:diaObjectId,r:scienceFlux,r:midpointMjdTai",
            required=False,
        ),
        "output-format": fields.String(
            description="Output format among json[default], csv, parquet, votable.",
            example="json",
            required=False,
        ),
    },
)


@ns.route("")
@ns.doc(params={k: ARGS[k].description for k in ARGS})
class Explore(Resource):
    def get(self):
        """Retrieve sample alert data from the Fink/LSST database based on their catalog class"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            out = extract_classes()
            return jsonify(out)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Retrieve sample alert data from the Fink/LSST database based on their catalog class"""
        # get payload from the query URL
        payload = request.args

        if payload is None or len(payload) == 0:
            # if no payload, try the JSON blob
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
