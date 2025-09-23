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
from flask import Response
import json
import requests

from line_profiler import profile


def flatten_nested(schema, entry_name):
    """ """
    blobs = [cat["type"] for cat in schema["fields"] if cat["name"] == entry_name][0]

    if isinstance(blobs, list):
        # nullable entry
        # Take the dict entry (non-null)
        mask = [isinstance(blob, dict) for blob in blobs]
        index = mask.index(True)
        dic = blobs[index]
    else:
        dic = blobs

    return dic["fields"]


def sort_dict(adict):
    """ """
    return {key: adict[key] for key in sorted(adict.keys())}


@profile
def extract_schema(payload: dict) -> Response:
    """Retrieve the data schema

    Notes
    -----
    All fields are initially defined in `fink_broker.rubin.hbase_utils`

    """
    # LSST candidate fields
    r = requests.get(
        "https://usdf-alert-schemas-dev.slac.stanford.edu/subjects/alert-packet/versions/latest/schema"
    )
    rubin_schema = r.json()

    # root level should be everywhere
    root_rubin_names = ["observation_reason", "target_name", "diaSourceId"]
    root_list = [i for i in rubin_schema["fields"] if i["name"] in root_rubin_names]

    # root level should be everywhere
    root_list.append(
        {
            "name": "fink_broker_version",
            "type": "string",
            "doc": "fink-broker schema version used",
        },
    )

    root_list.append(
        {
            "name": "fink_science_version",
            "type": "string",
            "doc": "fink-science schema version used",
        },
    )
    root_list.append(
        {
            "name": "lsst_schema_version",
            "type": "string",
            "doc": "LSST schema version used",
        },
    )

    # Fink Science modules
    fink_science = [
        {
            "name": "cdsxmatch",
            "type": "string",
            "doc": "Object type of the closest source from SIMBAD database; if exists within 1 arcsec. See https://api.fink-portal.org/api/v1/classes",
        },
        {
            "name": "gcvs",
            "type": "string",
            "doc": "Object type of the closest source from GCVS catalog; if exists within 1 arcsec.",
        },
        {
            "name": "vsx",
            "type": "string",
            "doc": "Object type of the closest source from VSX catalog; if exists within 1 arcsec.",
        },
        {
            "name": "DR3Name",
            "type": "string",
            "doc": "Unique source designation of closest source from Gaia catalog; if exists within 1 arcsec.",
        },
        {
            "name": "Plx",
            "type": "double",
            "doc": "Absolute stellar parallax (in milli-arcsecond) of the closest source from Gaia catalog; if exists within 1 arcsec.",
        },
        {
            "name": "e_Plx",
            "type": "double",
            "doc": "Standard error of the stellar parallax (in milli-arcsecond) of the closest source from Gaia catalog; if exists within 1 arcsec.",
        },
        {
            "name": "x3hsp",
            "type": "string",
            "doc": "Counterpart (cross-match) to the 3HSP catalog if exists within 1 arcminute.",
        },
        {
            "name": "x4lac",
            "type": "string",
            "doc": "Counterpart (cross-match) to the 4LAC DR3 catalog if exists within 1 arcminute.",
        },
        {
            "name": "mangrove_HyperLEDA_name",
            "type": "string",
            "doc": "HyperLEDA source designation of closest source from Mangrove catalog; if exists within 1 arcmin.",
        },
        {
            "name": "mangrove_2MASS_name",
            "type": "string",
            "doc": "2MASS source designation of closest source from Mangrove catalog; if exists within 1 arcmin.",
        },
        {
            "name": "mangrove_lum_dist",
            "type": "string",
            "doc": "Luminosity distance of closest source from Mangrove catalog; if exists within 1 arcmin.",
        },
        {
            "name": "mangrove_ang_dist",
            "type": "string",
            "doc": "Angular distance of closest source from Mangrove catalog; if exists within 1 arcmin.",
        },
        {
            "name": "spicy_id",
            "type": "string",
            "doc": "Unique source designation of closest source from SPICY catalog; if exists within 1.2 arcsec.",
        },
        {
            "name": "spicy_class",
            "type": "string",
            "doc": "Class name of closest source from SPICY catalog; if exists within 1.2 arcsec.",
        },
        {
            "name": "tns",
            "type": "string",
            "doc": "TNS label, if it exists.",
        },
        {
            "name": "gaiaClass",
            "type": "str",
            "doc": "Name of best class from Gaia DR3 Part 4. Variability (I/358/vclassre).",
        },
        {
            "name": "gaiaVarFlag",
            "type": "int",
            "doc": "Photometric variability flag from Gaia DR3. 1 if the source is variable, 0 otherwise.",
        },
    ]

    # # Sort by name
    # rubin_schema = rubin_schema.sort_values("name")
    # fink_science = fink_science.sort_values("name")

    # categories = [
    #     "diaSourceId",
    #     "observation_reason",
    #     "target_name",
    #     "diaSource",
    #     "prvDiaSources",
    #     "prvDiaForcedSources",
    #     "diaObject",
    #     "ssSource",
    #     "MPCORB",
    #     "cutoutDifference",
    #     "cutoutScience",
    #     "cutoutTemplate",
    # ]

    if payload["endpoint"] == "/api/v1/sources":
        # root, diaSOurce, fink
        types = {
            "Rubin original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in flatten_nested(rubin_schema, "diaSource") + root_list
                }
            ),
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in fink_science
                }
            ),
        }
    elif payload["endpoint"] == "/api/v1/objects":
        # root, diaObject, fink
        types = {
            "Rubin original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in flatten_nested(rubin_schema, "diaObject") + root_list
                }
            ),
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in fink_science
                }
            ),
        }
    elif payload["endpoint"] == "/api/v1/conesearch":
        # root, diaObject, fink
        types = {
            "Rubin original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in flatten_nested(rubin_schema, "diaSource") + flatten_nested(rubin_schema, "diaObject") + root_list
                }
            ),
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in fink_science
                }
            ),
        }

    # types = {
    #     "ZTF original fields (i:)": {
    #         i: {"type": j, "doc": k}
    #         for i, j, k in zip(
    #             rubin_schema.name, rubin_schema.type, rubin_schema.doc
    #         )
    #     },
    #     "ZTF original cutouts (b:)": {
    #         i: {"type": j, "doc": k}
    #         for i, j, k in zip(ztf_cutouts.name, ztf_cutouts.type, ztf_cutouts.doc)
    #     },
    #     "Fink science module outputs (d:)": {
    #         i: {"type": j, "doc": k}
    #         for i, j, k in zip(
    #             fink_science.name, fink_science.type, fink_science.doc
    #         )
    #     },
    #     "Fink on-the-fly added values (v:)": {
    #         i: {"type": j, "doc": k}
    #         for i, j, k in zip(
    #             fink_derived.name, fink_derived.type, fink_derived.doc
    #         )
    #     },
    # }

    response = Response(json.dumps(types), 200)
    response.headers.set("Content-Type", "application/json")
    return response
