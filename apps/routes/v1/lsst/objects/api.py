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
from flask import Response, request
from flask_restx import Namespace, Resource, fields

from apps.utils.utils import check_args
from apps.utils.utils import send_tabular_data

from apps.routes.v1.lsst.objects.utils import extract_object_data

ns = Namespace("api/v1/objects", "Get diaObject & aggregated data based on Rubin diaObjectId")

ARGS = ns.model(
    "objects",
    {
        "diaObjectId": fields.String(
            description='single Rubin Object ID, or a comma-separated list of object ID, e.g. "396895411240977"',
            example="396895411240977",
            required=True,
        ),
        "columns": fields.String(
            description="Comma-separated data columns to transfer, e.g. 'i:firstDiaSourceMjdTai,i:nDiaSources,i:g_psfFluxMax'. If not specified, transfer all columns.",
            example="i:firstDiaSourceMjdTai,i:nDiaSources,i:g_psfFluxMax",
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
class Objects(Resource):
    def get(self):
        """Retrieve object (aggregated) data from the Fink/LSST database based on their name"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            return Response(ns.description, 200)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Retrieve object (aggregated) data from the Fink/LSST database based on their name"""
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
