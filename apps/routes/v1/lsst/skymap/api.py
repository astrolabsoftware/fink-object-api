# Copyright 2024-2025 AstroLab Software
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

from apps.routes.v1.lsst.skymap.utils import search_in_skymap

ns = Namespace("api/v1/skymap", "Return Fink/LSST alerts within a GW skymap")

ARGS = ns.model(
    "skymap",
    {
        "file": fields.Raw(
            description="LIGO/Virgo probability sky maps, as gzipped FITS (bayestar.fits.gz). Not compatible with `event_name`.",
            required=False,
        ),
        "event_name": fields.String(
            description="If provided, directly query GraceDB with the `event_name`. Not compatible with the argument `file`.",
            example="S251112cm",
            required=False,
        ),
        "credible_level": fields.Float(
            description="GW credible region threshold to look for. Note that the values in the resulting credible level map vary inversely with probability density: the most probable pixel is assigned to the credible level 0.0, and the least likely pixel is assigned the credible level 1.0.",
            example=0.45,
            required=True,
        ),
        "n_day_before": fields.Float(
            description="Number of day(s) to search before the event. Default is 1, max is 7.",
            example=1.0,
            required=False,
        ),
        "n_day_after": fields.Float(
            description="Number of day(s) to search after the event. Default is 6, max is 14.",
            example=6.0,
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
class Skymap(Resource):
    def get(self):
        """Return Fink/LSST alerts within a GW skymap, within [-1 day, +6 days] for the event."""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            return Response(ns.description, 200)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Return Fink/LSST alerts within a GW skymap, within [-1 day, +6 days] for the event."""
        # get payload from the query URL
        payload = request.args

        if payload is None or len(payload) == 0:
            # if no payload, try the JSON blob
            payload = request.json

        rep = check_args(ARGS, payload)
        if rep["status"] != "ok":
            return Response(str(rep), 400)

        out = search_in_skymap(payload)

        # Error propagation
        if isinstance(out, Response):
            return out

        output_format = payload.get("output-format", "json")
        return send_tabular_data(out, output_format)
