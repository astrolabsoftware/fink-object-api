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
import pandas as pd
import requests

from apps.utils.client import connect_to_graph
from apps.utils.utils import extract_configuration

from line_profiler import profile


@profile
def extract_similar_objects(payload: dict) -> pd.DataFrame:
    """Extract similar objects returned by JanusGraph and format it in a Pandas dataframe

    Data is from /api/v1/recommender

    Parameters
    ----------
    payload: dict
        See https://api.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    if "n" not in payload:
        nobjects = 10
    else:
        nobjects = int(payload["n"])

    if "classifier" not in payload:
        classifier_name = "FINK_PORTAL"
    else:
        classifier_name = payload["classifier"]

    user_config = extract_configuration("config.yml")

    gr, classifiers = connect_to_graph()

    # Classify source
    func = getattr(classifiers, classifier_name)
    gr.classifySource(
        func,
        payload["objectId"],
        None,
        False,
        None,
    )

    closest_sources = gr.sourceNeighborhood(payload["objectId"], classifier_name, nobjects)
    out = {"i:objectId": [], "v:distance": [], "v:classification": []}
    for index, (k, _) in enumerate(closest_sources.items()):
        oid = k.getKey()
        distance = k.getValue()
        r = requests.post(
            "https://api.fink-portal.org/api/v1/objects",
            json={"objectId": oid, "output-format": "json"},
        )
        out["v:classification"].append(r.json()[0]["v:classification"])
        out["i:objectId"].append(oid)
        out["v:distance"].append(distance)

    pdf = pd.DataFrame(out)

    gr.close()

    return pdf
