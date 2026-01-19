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
from flask import Response, request, jsonify
from flask_restx import Namespace, Resource, fields

from apps.utils.utils import check_args
from apps.utils.utils import send_tabular_data

from apps.routes.v1.lsst.tags.utils import extract_object_data, extract_tags

ns = Namespace("api/v1/tags", "Get latest Rubin alerts by tags")

ARGS = ns.model(
    "class",
    {
        "tag": fields.String(
            description="Fink tags based on user-defined filters. See https://api.lsst.fink-portal.org/api/v1/classes for available tags.",
            example="cataloged",
            required=True,
        ),
        "n": fields.Integer(
            description="Last N alerts to transfer between stopping date and starting date. Default is 100.",
            example=10,
            required=False,
        ),
        "startdate": fields.String(
            description="Starting date in UTC (iso, jd, or MJD). Default is 2026-01-01 00:00:00",
            example="2026-01-01 00:00:00",
            required=False,
        ),
        "stopdate": fields.String(
            description="Stopping date in UTC (iso, jd, or MJD). Default is now.",
            example="2026-01-30 00:00:00",
            required=False,
        ),
        "columns": fields.String(
            description="Comma-separated data columns to transfer, e.g. 'i:magpsf,i:jd'. If not specified, transfer all columns.",
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
class Tags(Resource):
    def get(self):
        """Retrieve alert data from the Fink/LSST database based on their tag"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            return jsonify(extract_tags())

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Retrieve alert data from the Fink/LSST database based on their tag"""
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
