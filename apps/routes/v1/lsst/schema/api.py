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

from apps.routes.v1.lsst.schema.utils import extract_schema

ns = Namespace(
    "api/v1/schema", "Retrieve the data schema for a given endpoint for Fink/Rubin API"
)

ARGS = ns.model(
    "schema",
    {
        "endpoint": fields.String(
            description="Endpoint name",
            example="/api/v1/sources",
            required=True,
        ),
        "major_version": fields.Integer(
            description="LSST major version. Default is latest.",
            example=10,
            required=False,
        ),
        "minor_version": fields.Integer(
            description="LSST minor version. Default is latest.",
            example=0,
            required=False,
        ),
    },
)


@ns.route("")
@ns.doc(params={k: ARGS[k].description for k in ARGS})
class Schema(Resource):
    def get(self):
        """Retrieve the data schema for a given endpoint for Fink/Rubin API"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            # FIXME: return the list of endpoints?
            return Response(ns.description, 200)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Retrieve the data schema for a given endpoint for Fink/Rubin API"""
        # get payload from the query URL
        payload = request.args

        if payload is None or len(payload) == 0:
            # if no payload, try the JSON blob
            payload = request.json

        rep = check_args(ARGS, payload)
        if rep["status"] != "ok":
            return Response(str(rep), 400)

        out = extract_schema(payload)

        if isinstance(out, Response):
            return out
