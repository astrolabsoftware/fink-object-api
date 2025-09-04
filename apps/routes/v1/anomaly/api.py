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

from apps.routes.v1.ztf.anomaly.utils import get_anomalous_alerts

ns = Namespace("api/v1/anomaly", "Get alerts tagged as anomaly")

ARGS = ns.model(
    "anomaly",
    {
        "n": fields.Integer(
            description="Last N alerts to transfer between stop and start date (going from most recent to older alerts). Default is 10.",
            example=10,
            required=False,
        ),
        "start_date": fields.String(
            description="[Optional] Start date in UTC YYYY-MM-DD. Default is 2019-11-01.",
            example="2019-11-01",
            required=False,
        ),
        "stop_date": fields.String(
            description="[Optional] Stop date in UTC YYYY-MM-DD. Default is now.",
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
class Anomaly(Resource):
    def get(self):
        """Retrieve alerts tagged as anomaly from the Fink/ZTF database"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            return Response(ns.description, 200)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Retrieve alerts tagged as anomaly from the Fink/ZTF database"""
        # get payload from the query URL
        payload = request.args

        if payload is None or len(payload) == 0:
            # if no payload, try the JSON blob
            payload = request.json

        rep = check_args(ARGS, payload)
        if rep["status"] != "ok":
            return Response(str(rep), 400)

        out = get_anomalous_alerts(payload)

        # Error propagation
        if isinstance(out, Response):
            return out

        output_format = payload.get("output-format", "json")
        return send_tabular_data(out, output_format)
