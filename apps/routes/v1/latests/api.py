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
from flask import Response, request
from flask_restx import Namespace, Resource, fields

from apps.utils.utils import check_args
from apps.utils.utils import send_tabular_data

from apps.routes.v1.latests.utils import extract_object_from_class

ns = Namespace("api/v1/latests", "Get object data based their class")

ARGS = ns.model(
    "class",
    {
        "class": fields.String(
            description="Fink derived label. See https://api.fink-portal.org/api/v1/classes for available tags.",
            example="Early SN Ia candidate",
            required=True,
        ),
        "n": fields.Integer(
            description="Last N alerts to transfer between stopping date and starting date. Default is 100.",
            example=10,
            required=False,
        ),
        "startdate": fields.String(
            description="Starting date in UTC (iso, jd, or MJD). Default is 2019-11-01 00:00:00",
            example="2024-11-03 12:30:00",
            required=False,
        ),
        "stopdate": fields.String(
            description="Stopping date in UTC (iso, jd, or MJD). Default is now.",
            example="2024-12-03 12:30:00",
            required=False,
        ),
        "color": fields.Boolean(
            description="If True, extract color information for the transient. Default is False.",
            example=False,
            required=False,
        ),
        "columns": fields.String(
            description="Comma-separated data columns to transfer, e.g. 'i:magpsf,i:jd'. If not specified, transfer all columns.",
            example="i:objectId,i:jd,i:magpsf,i:fid",
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
class Latests(Resource):
    def get(self):
        """Retrieve object data from the Fink/ZTF database based on their Fink derived class"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            return Response(ns.description, 200)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Retrieve object data from the Fink/ZTF database based on their Fink derived class"""
        # get payload from the query URL
        payload = request.args

        if payload is None or len(payload) == 0:
            # if no payload, try the JSON blob
            payload = request.json

        rep = check_args(ARGS, payload)
        if rep["status"] != "ok":
            return Response(str(rep), 400)

        out = extract_object_from_class(payload)

        # Error propagation
        if isinstance(out, Response):
            return out

        output_format = payload.get("output-format", "json")
        return send_tabular_data(out, output_format)
