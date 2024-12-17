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

from apps.routes.objects.utils import extract_object_data

ns = Namespace("api/v1/objects", "Get object data based on ZTF ID")

ARGS = ns.model(
    "objects",
    {
        "objectId": fields.String(
            description='single ZTF Object ID, or a comma-separated list of object names, e.g. "ZTF19acmdpyr,ZTF21aaxtctv"',
            example="ZTF21abfmbix",
            required=True,
        ),
        "withupperlim": fields.Boolean(
            description="If True, retrieve also upper limit measurements, and bad quality measurements. Use the column `d:tag` in your results: valid, upperlim, badquality.",
            example=False,
            required=False,
        ),
        "withcutouts": fields.Boolean(
            description="If True, retrieve also cutout data as 2D array. See also `cutout-kind`. More information on the original cutouts at https://irsa.ipac.caltech.edu/data/ZTF/docs/ztf_explanatory_supplement.pdf",
            example=False,
            required=False,
        ),
        "cutout-kind": fields.String(
            description="`Science`, `Template`, or `Difference`. If not specified, returned all three.",
            example="Science",
            required=False,
        ),
        "columns": fields.String(
            description="Comma-separated data columns to transfer, e.g. 'i:magpsf,i:jd'. If not specified, transfer all columns.",
            example="i:jd,i:magpsf,i:fid",
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
        """Retrieve object data from the Fink/ZTF database"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            return Response(ns.description, 200)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Retrieve object data from the Fink/ZTF database"""
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
