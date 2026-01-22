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
import os
from flask import Flask, Blueprint
from flask_restx import Api
from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics
from config_prometheus import child_exit, pre_fork, post_fork
from prometheus_client import values
from prometheus_client.values import MultiProcessValue

from apps import __version__

from apps.utils.utils import extract_configuration

from apps.routes.v1.lsst.sources.api import ns as ns_sources
from apps.routes.v1.lsst.objects.api import ns as ns_objects
from apps.routes.v1.lsst.conesearch.api import ns as ns_conesearch
from apps.routes.v1.lsst.cutouts.api import ns as ns_cutouts
from apps.routes.v1.lsst.schema.api import ns as ns_schema
from apps.routes.v1.lsst.sso.api import ns as ns_sso
from apps.routes.v1.lsst.resolver.api import ns as ns_resolver
from apps.routes.v1.lsst.skymap.api import ns as ns_skymap
from apps.routes.v1.lsst.statistics.api import ns as ns_stats
from apps.routes.v1.lsst.tags.api import ns as ns_tags
from apps.routes.v1.lsst.blocks.api import ns as ns_blocks

config = extract_configuration("config.yml")


def get_worker_id():
    """return stable id for worker"""
    return os.environ.get("GUNICORN_WORKER_ID", str(os.getpid()))


# Overwrite ValueClass to use this id
values.ValueClass = MultiProcessValue(process_identifier=get_worker_id)

app = Flask("Fink/LSST REST API")
metrics = GunicornPrometheusMetrics(app, group_by="endpoint")

# calls for a properly defined child_exit, pre_fork, post_fork
metrics.child_exit = child_exit
metrics.pre_fork = pre_fork
metrics.post_fork = post_fork

# Master blueprint
blueprint = Blueprint("api", __name__, url_prefix="/")
api = Api(
    blueprint,
    version=__version__,
    title="Fink/LSST object API",
    description="REST API to access data from Fink/LSST",
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
api.add_namespace(ns_sources)
api.add_namespace(ns_objects)
api.add_namespace(ns_conesearch)
api.add_namespace(ns_cutouts)
api.add_namespace(ns_schema)
api.add_namespace(ns_sso)
api.add_namespace(ns_resolver)
api.add_namespace(ns_skymap)
api.add_namespace(ns_stats)
api.add_namespace(ns_tags)
api.add_namespace(ns_blocks)

# Register blueprint
app.register_blueprint(blueprint)


if __name__ == "__main__":
    app.run(config["HOST"], debug=True, port=int(config["PORT"]))
