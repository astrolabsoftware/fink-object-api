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
from flask import Flask

from apps.utils.utils import extract_configuration

from apps.routes.objects.api import bp as bp_objects
from apps.routes.cutouts.api import bp as bp_cutouts

config = extract_configuration("config.yml")

app = Flask("Fink REST API")

# Server configuration
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
app.config["JSON_SORT_KEYS"] = False

app.register_blueprint(bp_objects)
app.register_blueprint(bp_cutouts)

if __name__ == "__main__":
    app.run(config["HOST"], debug=True, port=int(config["PORT"]))
