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

from apps.routes.v1.lsst.cutouts.utils import format_and_send_cutout

ns = Namespace("api/v1/cutouts", "Get cutout data based on Rubin diaObjectId")

ARGS = ns.model(
    "cutouts",
    {
        "diaObjectId": fields.String(
            description="Rubin Object ID",
            example="169298438257115164",
            required=True,
        ),
        "kind": fields.String(
            description="Science, Template, or Difference. For output-format=array, you can also specify `kind: All` to get the 3 cutouts.",
            example="Science",
            required=True,
        ),
        "output-format": fields.String(
            description="PNG[default], FITS, array", example="PNG", required=False
        ),
        "diaSourceId": fields.String(
            description="diaSource ID (long integer) of the alert belonging to the object with `diaObjectId`. If not filled, the cutouts of the latest alert is returned",
            example="169298437355340113",
            required=False,
        ),
        "stretch": fields.String(
            description="Stretch function to be applied. Available: sigmoid[default], linear, sqrt, power, log.",
            example="sigmoid",
            required=False,
        ),
        "colormap": fields.String(
            description="Valid matplotlib colormap name (see matplotlib.cm). Default is grayscale.",
            example="Blues",
            required=False,
        ),
        "pmin": fields.Float(
            description="The percentile value used to determine the pixel value of minimum cut level. Default is 0.5. No effect for sigmoid.",
            example=0.5,
            required=False,
        ),
        "pmax": fields.Float(
            description="The percentile value used to determine the pixel value of maximum cut level. Default is 99.5. No effect for sigmoid.",
            example=99.5,
            required=False,
        ),
        "convolution_kernel": fields.String(
            description="Convolve the image with a kernel (gauss or box). If not specified, no kernel is applied.",
            example="gauss",
            required=False,
        ),
    },
)


@ns.route("")
@ns.doc(params={k: ARGS[k].description for k in ARGS})
class Cutouts(Resource):
    def get(self):
        """Retrieve cutout data from the Fink/Rubin datalake"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            return Response(ns.description, 200)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Retrieve cutout data from the Fink/Rubin datalake"""
        # get payload from the query URL
        payload = request.args

        if payload is None or len(payload) == 0:
            # if no payload, try the JSON blob
            payload = request.json

        rep = check_args(ARGS, payload)
        if rep["status"] != "ok":
            return Response(str(rep), 400)

        assert payload["kind"] in ["Science", "Template", "Difference", "All"]

        return format_and_send_cutout(payload)
