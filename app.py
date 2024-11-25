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
from flask import Flask, url_for
import yaml

config = yaml.load(open("config.yml"), yaml.Loader)

app = Flask("Fink REST API")

# HBase client nrows default limit
nlimit = 10000

# Server configuration
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
app.config["JSON_SORT_KEYS"] = False

from apps.template.api import bp as bp_template
app.register_blueprint(bp_template)

if __name__ == "__main__":
    app.run(config["APIURL"], debug=True, port=int(config["PORT"]))
