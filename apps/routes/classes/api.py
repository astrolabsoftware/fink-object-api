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
import pandas as pd

from flask import Response
from flask_restx import Namespace, Resource

from fink_utils.xmatch.simbad import get_simbad_labels

ns = Namespace("api/v1/classes", "Get Fink derived class names, and their origin")


@ns.route("")
class Classnames(Resource):
    def get(self):
        """Retrieve all Fink derived class names, and their origin"""
        # TNS
        tns_types = pd.read_csv("assets/tns_types.csv", header=None)[0].to_numpy()
        tns_types = sorted(tns_types, key=lambda s: s.lower())
        tns_types = ["(TNS) " + x for x in tns_types]

        # SIMBAD
        simbad_types = get_simbad_labels("old_and_new")
        simbad_types = sorted(simbad_types, key=lambda s: s.lower())
        simbad_types = ["(SIMBAD) " + x for x in simbad_types]

        # Fink science modules
        fink_types = pd.read_csv("assets/fink_types.csv", header=None)[0].to_numpy()
        fink_types = sorted(fink_types, key=lambda s: s.lower())

        types = {
            "Fink classifiers": fink_types,
            "Cross-match with TNS": tns_types,
            "Cross-match with SIMBAD (see http://simbad.u-strasbg.fr/simbad/sim-display?data=otypes)": simbad_types,
        }

        return Response({"classes": types}, 200)
