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
from flask import jsonify
from flask_restx import Namespace, Resource

from apps.routes.v1.lsst.blocks.utils import extract_blocks

ns = Namespace("api/v1/blocks", "Get blocks definition")


@ns.route("")
class Blocks(Resource):
    def get(self):
        """Retrieve block definition"""
        tags, descriptions = extract_blocks(True)
        out = {k: v for k, v in zip(tags, descriptions)}
        return jsonify(out)
