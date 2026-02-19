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
from flask import Response, request
from flask_restx import Namespace, Resource, fields

from pandas import DataFrame

from apps.utils.utils import check_args
from apps.utils.utils import send_tabular_data

from apps.routes.v1.ztf.ssobulk.utils import get_lc

ns = Namespace("api/v1/ssobulk", "Get all Fink/ZTF SSO lightcurves in once")

ARGS = ns.model(
    "ssobulk",
    {
        "output-format": fields.String(
            description="Output format among json, csv, parquet[default], votable.",
            example="parquet",
            required=False,
        ),
    },
)


@ns.route("")
@ns.doc(params={k: ARGS[k].description for k in ARGS})
class Ssobulk(Resource):
    def get(self):
        """Get all Fink/ZTF SSO lightcurves in once"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            return Response(ns.description, 200)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Get all Fink/ZTF SSO lightcurves in once"""
        # get payload from the query URL
        payload = request.args

        if payload is None or len(payload) == 0:
            # if no payload, try the JSON blob
            payload = request.json

        rep = check_args(ARGS, payload)
        if rep["status"] != "ok":
            return Response(str(rep), 400)

        out = get_lc(payload)

        # Error propagation
        if isinstance(out, Response):
            return out

        # Return a record
        if isinstance(out, DataFrame):
            output_format = payload.get("output-format", "json")
            return send_tabular_data(out, output_format)

        # return the full table as binary
        return out
