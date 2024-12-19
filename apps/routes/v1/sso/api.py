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

from apps.routes.v1.sso.utils import extract_sso_data

from apps.utils.utils import check_args
from apps.utils.utils import send_tabular_data


ns = Namespace("api/v1/sso", "Get Solar System object data based on their ID")

ARGS = ns.model(
    "sso",
    {
        "n_or_d": fields.String(
            description="IAU number of the object, or designation of the object IF the number does not exist yet. Example for numbers: 8467 (asteroid) or 10P (comet). Example for designations: 2010JO69 (asteroid) or C/2020V2 (comet). You can also give a list of object names (comma-separated).",
            example="8467",
            required=True,
        ),
        "withEphem": fields.Boolean(
            description="Attach ephemerides provided by the Miriade service (https://ssp.imcce.fr/webservices/miriade/api/ephemcc/), as extra columns in the results.",
            example=False,
            required=False,
        ),
        "withResiduals": fields.Boolean(
            description="Return the residuals `obs - model` using the sHG1G2 phase curve model. Work only for a single object query (`n_or_d` cannot be a list).",
            example=False,
            required=False,
        ),
        "withCutouts": fields.Boolean(
            description="If True, retrieve also cutout data as 2D array. See also `cutout-kind`. More information on the original cutouts at https://irsa.ipac.caltech.edu/data/ZTF/docs/ztf_explanatory_supplement.pdf",
            example=False,
            required=False,
        ),
        "cutout-kind": fields.String(
            description="`Science`, `Template`, or `Difference`.",
            example="Science",
            required=False,
        ),
        "columns": fields.String(
            description="Comma-separated data columns to transfer, e.g. 'i:magpsf,i:jd'. If not specified, transfer all columns.",
            example="i:jd,i:magpsf,i:fid,i:ssnamenr",
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
class Solarsystem(Resource):
    def get(self):
        """Retrieve Solar System data from the Fink/ZTF database based on their ID"""
        payload = request.args
        if len(payload) > 0:
            # POST from query URL
            return self.post()
        else:
            return Response(ns.description, 200)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Retrieve Solar System data from the Fink/ZTF database based on their ID"""
        # get payload from the query URL
        payload = request.args

        if payload is None or len(payload) == 0:
            # if no payload, try the JSON blob
            payload = request.json

        rep = check_args(ARGS, payload)
        if rep["status"] != "ok":
            return Response(str(rep), 400)

        out = extract_sso_data(payload)

        # Error propagation
        if isinstance(out, Response):
            return out

        output_format = payload.get("output-format", "json")
        return send_tabular_data(out, output_format)
