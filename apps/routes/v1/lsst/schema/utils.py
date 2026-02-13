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
import logging

from line_profiler import profile

_LOG = logging.getLogger(__name__)


def sort_dict(adict):
    """ """
    return {key: adict[key] for key in sorted(adict.keys())}


def replace_hyphen_for_name(dic):
    elem = dic.copy()
    elem["name"] = elem["name"].replace("_", ".", 1)
    return elem


def add_prefix_section(dic, prefix=""):
    elem = dic.copy()
    # FIXME: to be removed whn project will fill it
    if elem["name"] == "firstDiaSourceMjdTaiFink":
        elem["name"] = "misc." + elem["name"]
    else:
        elem["name"] = prefix + elem["name"]
    return elem


def reconstruct_fink_schema(fink_source, fink_object):
    """ """
    fink_source_reconstructed = [replace_hyphen_for_name(dic) for dic in fink_source]
    fink_object_reconstructed = [
        add_prefix_section(dic, "pred.") for dic in fink_object
    ]

    return fink_source_reconstructed, fink_object_reconstructed


def reconstruct_lsst_schema(section, name):
    """ """
    section_reconstructed = [add_prefix_section(dic, name) for dic in section]
    return section_reconstructed


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
    diaSource_schema = r_diaSource.json()["fields"]

    r_diaObject = requests.get(
        "{}/{}/{}/lsst.v{}_{}.diaObject.avsc".format(
            base_url, major_version, minor_version, major_version, minor_version
        )
    )
    diaObject_schema = r_diaObject.json()["fields"]

    r_ssSource = requests.get(
        "{}/{}/{}/lsst.v{}_{}.ssSource.avsc".format(
            base_url, major_version, minor_version, major_version, minor_version
        )
    )
    ssSource_schema = r_ssSource.json()["fields"]

    r_mpc_orbits = requests.get(
        "{}/{}/{}/lsst.v{}_{}.mpc_orbits.avsc".format(
            base_url, major_version, minor_version, major_version, minor_version
        )
    )
    mpc_orbits_schema = r_mpc_orbits.json()["fields"]

    # Fink Science modules
    # Store this on disk as avsc - versioned.
    fink_source_science = [
        {
            "name": "xm_simbad_otype",
            "type": "string",
            "doc": "Object type of the closest source from SIMBAD database; if exists within 1 arcsec. See https://api.lsst.fink-portal.org/api/v1/classes",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_gcvs_type",
            "type": "string",
            "doc": "Object type of the closest source from GCVS catalog; if exists within 1 arcsec.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_vsx_Type",
            "type": "string",
            "doc": "Object type of the closest source from VSX catalog; if exists within 1 arcsec.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_gaiadr3_DR3Name",
            "type": "string",
            "doc": "Unique source designation of closest source from Gaia catalog; if exists within 1 arcsec.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_gaiadr3_Plx",
            "type": "double",
            "doc": "Absolute stellar parallax (in milli-arcsecond) of the closest source from Gaia catalog; if exists within 1 arcsec.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_gaiadr3_e_Plx",
            "type": "double",
            "doc": "Standard error of the stellar parallax (in milli-arcsecond) of the closest source from Gaia catalog; if exists within 1 arcsec.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_x3hsp_type",
            "type": "string",
            "doc": "Counterpart (cross-match) to the 3HSP catalog if exists within 1 arcminute.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_x4lac_type",
            "type": "string",
            "doc": "Counterpart (cross-match) to the 4LAC DR3 catalog if exists within 1 arcminute.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_mangrove_HyperLEDA_name",
            "type": "string",
            "doc": "HyperLEDA source designation of closest source from Mangrove catalog; if exists within 1 arcmin.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_mangrove_2MASS_name",
            "type": "string",
            "doc": "2MASS source designation of closest source from Mangrove catalog; if exists within 1 arcmin.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_mangrove_lum_dist",
            "type": "string",
            "doc": "Luminosity distance of closest source from Mangrove catalog; if exists within 1 arcmin.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_mangrove_ang_dist",
            "type": "string",
            "doc": "Angular distance of closest source from Mangrove catalog; if exists within 1 arcmin.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_spicy_SPICY",
            "type": "string",
            "doc": "Unique source designation of closest source from SPICY catalog; if exists within 1.2 arcsec.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_spicy_class",
            "type": "string",
            "doc": "Class name of closest source from SPICY catalog; if exists within 1.2 arcsec.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_tns_type",
            "type": "string",
            "doc": "TNS label, if it exists.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_tns_fullname",
            "type": "string",
            "doc": "TNS name, if it exists.",
            "fink_broker_version": "4.1",
            "fink_science_version": "8.36.0",
        },
        {
            "name": "xm_tns_redshift",
            "type": "float",
            "doc": "Redshift from TNS, if it exists.",
            "fink_broker_version": "4.1",
            "fink_science_version": "8.36.0",
        },
        {
            "name": "xm_gaiadr3_VarFlag",
            "type": "int",
            "doc": "Photometric variability flag from Gaia DR3. 1 if the source is variable, 0 otherwise.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "xm_zphot",
            "type": "float",
            "doc": "Photo-z estimate from Legacy Surveys DR8 South Photometric Redshifts catalog - mean of the normally distributed photo-z posterior",
            "fink_broker_version": "4.1",
            "fink_science_version": "8.34.0",
        },
        {
            "name": "xm_e_zphot",
            "type": "float",
            "doc": "Uncertainty on zphot from Legacy Surveys DR8 South Photometric Redshifts catalog - standard deviation of the normally distributed photo-z posterior.",
            "fink_broker_version": "4.1",
            "fink_science_version": "8.34.0",
        },
        {
            "name": "xm_pstar",
            "type": "float",
            "doc": "Star likelihood based on colours from GMM star-QSO classification (Legacy Surveys DR8 South Photometric Redshifts catalog)",
            "fink_broker_version": "4.1",
            "fink_science_version": "8.34.0",
        },
        {
            "name": "xm_fqual",
            "type": "int",
            "doc": "Photo-z reliability flag from Legacy Surveys DR8 South Photometric Redshifts catalog. =1 for sources expected to have well-constrained estimates",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "clf_cats_class",
            "type": "int",
            "doc": "CATS classifier broad class prediction with the highest probability. -1= not processed, 11=SN-like, 12=Fast (e.g. KN, ulens, Novae, ...), 13=Long (e.g. SLSN, TDE, ...), 21=Periodic (e.g. RRLyrae, EB, ...), 22=Non-periodic (e.g. AGN). See https://arxiv.org/abs/2404.08798",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "clf_cats_score",
            "type": "float",
            "doc": "CATS classifier highest probability (0...1). See https://arxiv.org/abs/2404.08798",
            "fink_broker_version": "4.1",
            "fink_science_version": "8.35.0",
        },
        {
            "name": "clf_earlySNIa_score",
            "type": "float",
            "doc": "Score (0...1) for the early SN Ia classifier (binary classifier). See https://arxiv.org/abs/2404.08798",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "clf_slsn_score",
            "type": "float",
            "doc": "Score (0...1) for the super-luminous SN classifier (binary classifier). See https://arxiv.org/abs/2404.08798",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "clf_snnSnVsOthers_score",
            "type": "float",
            "doc": "Score (0...1) for the SN classifier (binary classifier) using SuperNNova. See https://arxiv.org/abs/2404.08798",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "clf_elephant_kstest_science",
            "type": "float",
            "doc": "hostless indicator in the science image from the ELEPHANT pipeline. See https://arxiv.org/abs/2404.18165",
            "fink_broker_version": "4.1",
            "fink_science_version": "8.34.0",
        },
        {
            "name": "clf_elephant_kstest_template",
            "type": "float",
            "doc": "hostless indicator in the template image from the ELEPHANT pipeline. See https://arxiv.org/abs/2404.18165",
            "fink_broker_version": "4.1",
            "fink_science_version": "8.34.0",
        },
        {
            "name": "fink_broker_version",
            "type": "string",
            "doc": "fink-broker schema version used to process the alert",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "fink_science_version",
            "type": "string",
            "doc": "fink-science schema version used to process the alert",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "lsst_schema_version",
            "type": "string",
            "doc": "LSST schema version used to generate the alert",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
    ]

    fink_object_science = [
        {
            "name": "is_cataloged",
            "type": "boolean",
            "doc": "True if the last diaSource (alert) of the diaObject (object) has a counterpart in either SIMBAD or Gaia DR3. False otherwise.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "is_sso",
            "type": "boolean",
            "doc": "True if the diaSource is associate to a known Solar System object. False otherwise.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "is_first",
            "type": "boolean",
            "doc": "True if the alert is not a Solar System object and has no history (first detection at this location).",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "main_label_classifier",
            "type": "int",
            "doc": "Main prediction from Fink classifiers for the last received alert of this object. This is currently set to the CATS prediction only (f:clf_cats_class). Subject to change.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "main_label_crossmatch",
            "type": "string",
            "doc": "Main association from various crossmatches for the last received alert of this object. This is currently set to the SIMBAD label only (f:xm_simbad_otype). Subject to change.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "firstDiaSourceMjdTaiFink",
            "type": "string",
            "doc": "MJD for the first detection by Rubin. Temporary replacement for diaObject.firstDiaSourceMjdTai which is not yet populated by the project",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
    ]

    fink_statistics = [
        {
            "name": "night",
            "type": "int",
            "doc": "Observation date in the form YYYYMMDD",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "alerts",
            "type": "int",
            "doc": "Number of alerts processed",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "alerts_u",
            "type": "int",
            "doc": "Number of alerts processed for band u",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "alerts_g",
            "type": "int",
            "doc": "Number of alerts processed for band g",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "alerts_r",
            "type": "int",
            "doc": "Number of alerts processed for band r",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "alerts_i",
            "type": "int",
            "doc": "Number of alerts processed for band i",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "alerts_z",
            "type": "int",
            "doc": "Number of alerts processed for band z",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "alerts_y",
            "type": "int",
            "doc": "Number of alerts processed for band y",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "objects",
            "type": "int",
            "doc": "Number of unique objects for the night",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "is_sso",
            "type": "int",
            "doc": "Number of alerts associated to a Solar System objects",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "is_first",
            "type": "int",
            "doc": "Number of alerts with first detection",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "in_tns",
            "type": "int",
            "doc": "Number of alerts with a counterpart in TNS",
            "fink_broker_version": "4.1",
            "fink_science_version": "8.34.0",
        },
        {
            "name": "is_cataloged",
            "type": "int",
            "doc": "Number of alerts with a counterpart in SIMBAD or Gaia DR3.",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "visits",
            "type": "int",
            "doc": "Number of visits",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "fink_broker_version",
            "type": "str",
            "doc": "fink-broker version used to process the alert data",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "fink_science_version",
            "type": "str",
            "doc": "fink-science version used to process the alert data",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
        {
            "name": "lsst_schema_version",
            "type": "str",
            "doc": "LSST schema version used to generate alert data",
            "fink_broker_version": "4.0",
            "fink_science_version": "8.26.0",
        },
    ]

    if payload["endpoint"] == "/api/v1/sources":
        # root, diaSOurce, fink
        types = {
            "LSST original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in diaSource_schema + root_list
                }
            ),
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {
                        "type": i["type"],
                        "doc": i.get("doc", "TBD"),
                        "fink_broker_version": i["fink_broker_version"],
                        "fink_science_version": i["fink_science_version"],
                    }
                    for i in fink_source_science
                }
            ),
        }
    elif payload["endpoint"] == "/api/v1/objects":
        # root, diaObject, fink
        types = {
            "LSST original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in diaObject_schema + root_list
                }
            ),
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {
                        "type": i["type"],
                        "doc": i.get("doc", "TBD"),
                        "fink_broker_version": i["fink_broker_version"],
                        "fink_science_version": i["fink_science_version"],
                    }
                    for i in fink_object_science
                }
            ),
        }
    elif payload["endpoint"] == "/api/v1/conesearch":
        types = {
            "LSST original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in diaSource_schema + diaObject_schema + root_list
                }
            ),
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {
                        "type": i["type"],
                        "doc": i.get("doc", "TBD"),
                        "fink_broker_version": i["fink_broker_version"],
                        "fink_science_version": i["fink_science_version"],
                    }
                    for i in fink_source_science + fink_object_science
                }
            ),
        }
    elif payload["endpoint"] == "/api/v1/cutouts":
        types = {
            "LSST original cutouts (b:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in cutout_list
                }
            ),
        }
    elif payload["endpoint"] == "/api/v1/sso":
        # FIXME: where mpc_orbits goes???
        types = {
            "LSST original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in ssSource_schema + diaSource_schema + root_list
                }
            ),
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {
                        "type": i["type"],
                        "doc": i.get("doc", "TBD"),
                        "fink_broker_version": "4.0",
                        "fink_science_version": "8.26.0",
                    }
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
    elif payload["endpoint"] == "/api/v1/tags":
        types = {
            "LSST original fields (r:)": sort_dict(
                {
                    i["name"]: {"type": i["type"], "doc": i.get("doc", "TBD")}
                    for i in diaSource_schema + root_list
                }
            ),
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {
                        "type": i["type"],
                        "doc": i.get("doc", "TBD"),
                        "fink_broker_version": i["fink_broker_version"],
                        "fink_science_version": i["fink_science_version"],
                    }
                    for i in fink_source_science
                }
            ),
        }
    elif payload["endpoint"] == "/api/v1/statistics":
        types = {
            "Fink science module outputs (f:)": sort_dict(
                {
                    i["name"]: {
                        "type": i["type"],
                        "doc": i.get("doc", "TBD"),
                        "fink_broker_version": i["fink_broker_version"],
                        "fink_science_version": i["fink_science_version"],
                    }
                    for i in fink_statistics
                }
            ),
        }
    elif payload["endpoint"] == "/datatransfer/fink":
        fink_source_science_reconstructed, fink_object_science_reconstructed = (
            reconstruct_fink_schema(fink_source_science, fink_object_science)
        )
        types = {
            "Fink": sort_dict(
                {
                    i["name"]: {
                        "type": i["type"],
                        "doc": i.get("doc", "TBD"),
                        "fink_broker_version": i["fink_broker_version"],
                        "fink_science_version": i["fink_science_version"],
                    }
                    for i in fink_source_science_reconstructed
                    + fink_object_science_reconstructed
                }
            ),
        }
    elif payload["endpoint"] == "/datatransfer/lsst":
        all_fields = (
            root_list
            + reconstruct_lsst_schema(diaObject_schema, "diaObject.")
            + reconstruct_lsst_schema(diaSource_schema, "diaSource.")
            + reconstruct_lsst_schema(ssSource_schema, "ssSource.")
            + reconstruct_lsst_schema(mpc_orbits_schema, "mpc_orbits.")
        )
        types = {
            "LSST": sort_dict(
                {
                    i["name"]: {
                        "type": i["type"],
                        "doc": i.get("doc", "TBD"),
                    }
                    for i in all_fields
                }
            ),
        }
    else:
        # FIXME: /gw is missing...
        msg = "{} is not a valid endpoint".format(payload["endpoint"])
        _LOG.warning(msg)
        return Response(msg, 404)

    response = Response(json.dumps(types), 200)
    response.headers.set("Content-Type", "application/json")
    return response
