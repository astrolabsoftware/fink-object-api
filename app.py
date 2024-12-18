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
from flask import Flask, Blueprint
from flask_restx import Api

from apps import __version__

from apps.utils.utils import extract_configuration

from apps.routes.v1.objects.api import ns as ns_objects
from apps.routes.v1.cutouts.api import ns as ns_cutouts
from apps.routes.v1.latests.api import ns as ns_latests
from apps.routes.v1.classes.api import ns as ns_classes
from apps.routes.v1.conesearch.api import ns as ns_conesearch
from apps.routes.v1.sso.api import ns as ns_sso

config = extract_configuration("config.yml")

app = Flask("Fink REST API")

# Master blueprint
blueprint = Blueprint("api", __name__, url_prefix="/")
api = Api(
    blueprint,
    version=__version__,
    title="Fink object API",
    description="REST API to access data from Fink",
)


# Enable CORS for this blueprint
@blueprint.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
    return response


# Server configuration
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
app.config["JSON_SORT_KEYS"] = False

# Register namespace
api.add_namespace(ns_objects)
api.add_namespace(ns_cutouts)
api.add_namespace(ns_latests)
api.add_namespace(ns_classes)
api.add_namespace(ns_conesearch)
api.add_namespace(ns_sso)

# Register blueprint
app.register_blueprint(blueprint)


if __name__ == "__main__":
    app.run(config["HOST"], debug=True, port=int(config["PORT"]))
