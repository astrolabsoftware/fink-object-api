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
    if ("major_version" not in payload) or ("minor_version" not in payload):
        # Get latest version
        r = requests.get(
            "https://raw.githubusercontent.com/lsst/alert_packet/refs/heads/main/python/lsst/alert/packet/schema/latest.txt"
        )
        version = "{}".format(r.json())
        major_version, minor_version = [int(i) for i in version.split(".")]
    else:
        major_version = payload["major_version"]
        minor_version = payload["minor_version"]

    base_url = "https://raw.githubusercontent.com/lsst/alert_packet/refs/heads/main/python/lsst/alert/packet/schema"

    r_root = requests.get(
        "{}/{}/{}/lsst.v{}_{}.alert.avsc".format(
            base_url, major_version, minor_version, major_version, minor_version
        )
    )
    root_schema = r_root.json()

    # root level should be everywhere
    root_rubin_names = ["observation_reason", "target_name", "diaSourceId"]
    root_list = [i for i in root_schema["fields"] if i["name"] in root_rubin_names]

    cutout_rubin_names = ["cutoutDifference", "cutoutTemplate", "cutoutScience"]
    cutout_list = [i for i in root_schema["fields"] if i["name"] in cutout_rubin_names]

    # Other fields
    r_diaSource = requests.get(
        "{}/{}/{}/lsst.v{}_{}.diaSource.avsc".format(
            base_url, major_version, minor_version, major_version, minor_version
        )
    )
    diaSource_schema = r_diaSource.json()

    r_diaObject = requests.get(
        "{}/{}/{}/lsst.v{}_{}.diaObject.avsc".format(
            base_url, major_version, minor_version, major_version, minor_version
        )
    )
    diaObject_schema = r_diaObject.json()

    r_ssSource = requests.get(
        "{}/{}/{}/lsst.v{}_{}.ssSource.avsc".format(
            base_url, major_version, minor_version, major_version, minor_version
        )
    )
    ssSource_schema = r_ssSource.json()

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
        {
            "name": "fink_broker_version",
            "type": "string",
            "doc": "fink-broker schema version used",
        },
        {
            "name": "fink_science_version",
            "type": "string",
            "doc": "fink-science schema version used",
        },
        {
            "name": "lsst_schema_version",
            "type": "string",
            "doc": "LSST schema version used",
        },
    ]

    if payload["endpoint"] == "/api/v1/sources":
        # root, diaSOurce, fink
        types = {
            "Rubin original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in diaSource_schema + root_list
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
                    for i in diaObject_schema + root_list
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
        types = {
            "Rubin original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in diaSource_schema + diaObject_schema + root_list
                }
            ),
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in fink_science
                }
            ),
        }
    elif payload["endpoint"] == "/api/v1/cutouts":
        types = {
            "Rubin original cutouts (b:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in cutout_list
                }
            ),
        }
    elif payload["endpoint"] == "/api/v1/sso":
        types = {
            "Rubin original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in ssSource_schema + root_list
                }
            ),
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in [
                        {
                            "name": "sso_name",
                            "type": "str",
                            "doc": "Resolved name from quaero",
                        }  # Manual entry from the /api/v1/sso route
                    ]
                }
            ),
        }

    response = Response(json.dumps(types), 200)
    response.headers.set("Content-Type", "application/json")
    return response
