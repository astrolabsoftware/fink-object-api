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

from apps.routes.v1.metadata.utils import post_metadata, retrieve_metadata, retrieve_oid

ns = Namespace("api/v1/metadata", "Get user-defined metadata attached to objects")

ARGS = ns.model(
    "metadata",
    {
        "objectId": fields.String(
            description="single ZTF Object ID.",
            example="ZTF23aaaatwl",
            required=True,
        ),
        "internal_name": fields.String(
            description="Internal name to be given.", required=True
        ),
        "username": fields.String(
            description="The username of the submitter.",
            required=True,
        ),
        "comments": fields.String(
            description="Any relevant comments for the object.", required=False
        ),
    },
)


@ns.route("")
@ns.doc(False)
class Metadata(Resource):
    def get(self):
        """Get user-defined metadata attached to an object"""
        payload = request.args
        if len(payload) == 0:
            return Response(ns.description, 200)
        elif len(payload) == 1:
            if "objectId" in payload:
                # return the associated data
                pdf = retrieve_metadata(payload["objectId"])
            elif "internal_name" in payload:
                # return the associated data
                pdf = retrieve_oid(payload["internal_name"], "internal_name")
            elif "internal_name_encoded" in payload:
                # return the associated data
                pdf = retrieve_oid(
                    payload["internal_name_encoded"], "internal_name_encoded"
                )
            else:
                rep = {
                    "status": "error",
                    "text": "Argument not understood. Choose one among: `objectId`, `internal_name`, `internal_name_encoded`.",
                }
                return Response(rep, 400)

            return send_tabular_data(pdf, "json")

        else:
            rep = {
                "status": "error",
                "text": "Too many arguments! Choose one among: `objectId`, `internal_name`, `internal_name_encoded`",
            }
            return Response(rep, 400)

    @ns.expect(ARGS, location="json", as_dict=True)
    def post(self):
        """Attach user-defined metadata to an object"""
        # get payload from the query URL
        payload = request.args

        if payload is None or len(payload) == 0:
            # if no payload, try the JSON blob
            payload = request.json

        rep = check_args(ARGS, payload)
        if rep["status"] != "ok":
            return Response(str(rep), 400)

        out = post_metadata(payload)

        # Error propagation
        if isinstance(out, Response):
            return out

        return send_tabular_data(out, "json")
