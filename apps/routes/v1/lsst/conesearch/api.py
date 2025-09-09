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

from apps.routes.v1.lsst.conesearch.utils import run_conesearch

ns = Namespace("api/v1/conesearch", "Get Rubin alerts data based on coordinates")

ARGS = ns.model(
    "conesearch",
    {
        "ra": fields.Float(
            description="Right Ascension, in decimal degree.",
            example=7.893627,
            required=True,
        ),
        "dec": fields.Float(
            description="Declination, in decimal degree.",
            example=-44.771556,
            required=True,
        ),
        "radius": fields.Float(
            description="Conesearch radius in arcsec. Maximum is 18,000 arcseconds (5 degrees).",
            example=10.0,
            required=True,
        ),
        "n": fields.Integer(
            description="Maximal number of alerts to return. Default is 1000.",
            example=10,
            required=False,
        ),
        "startdate": fields.String(
            description="Starting date in UTC (iso, jd, or MJD). It restricts the search to alerts whose first detection was within the specified range of dates and NOT all transients seen during this period. Default is 2025-09-06 00:00:00",
            example="2025-09-05 12:30:00",
            required=False,
        ),
        "stopdate": fields.String(
            description="Stopping date in UTC (iso, jd, or MJD). It restricts the search to alerts whose first detection was within the specified range of dates and NOT all transients seen during this period. Default is now.",
            example="2025-09-15 12:30:00",
            required=False,
        ),
        "window": fields.Float(
            description="Time window in days, may be used instead of stopdate. It restricts the search to alerts whose first detection was within the specified range of dates and NOT all transients seen during this period.",
            required=False,
        ),
        "columns": fields.String(
            description="Comma-separated data columns to transfer, e.g. 'i:midpointMjdTai,i:psfFlux,i:band'. If not specified, transfer all columns.",
            example="i:midpointMjdTai,i:psfFlux,i:band",
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
        """Retrieve lightcurve data from the Fink/LSST database based on coordinates"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            return Response(ns.description, 200)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Retrieve lightcurve data from the Fink/LSST database based on coordinates"""
        # get payload from the query URL
        payload = request.args

        if payload is None or len(payload) == 0:
            # if no payload, try the JSON blob
            payload = request.json

        rep = check_args(ARGS, payload)
        if rep["status"] != "ok":
            return Response(str(rep), 400)

        out = run_conesearch(payload)

        # Error propagation
        if isinstance(out, Response):
            return out

        output_format = payload.get("output-format", "json")
        return send_tabular_data(out, output_format)
