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
import requests
import pandas as pd

from flask import Response, json
from flask_restx import Namespace, Resource

ns = Namespace("api/v1/schema", "Get the data schema")


@ns.route("")
class Schema(Resource):
    def get(self):
        """Retrieve the data schema"""
        # ZTF candidate fields
        r = requests.get(
            "https://raw.githubusercontent.com/ZwickyTransientFacility/ztf-avro-alert/master/schema/candidate.avsc"
        )
        tmp = pd.DataFrame.from_dict(r.json())
        ztf_candidate = tmp["fields"].apply(pd.Series)
        ztf_candidate = ztf_candidate._append(
            {
                "name": "schemavsn",
                "type": "string",
                "doc": "schema version used",
            },
            ignore_index=True,
        )
        ztf_candidate = ztf_candidate._append(
            {
                "name": "publisher",
                "type": "string",
                "doc": "origin of alert packet",
            },
            ignore_index=True,
        )
        ztf_candidate = ztf_candidate._append(
            {
                "name": "objectId",
                "type": "string",
                "doc": "object identifier or name",
            },
            ignore_index=True,
        )

        ztf_candidate = ztf_candidate._append(
            {
                "name": "fink_broker_version",
                "type": "string",
                "doc": "Fink broker (fink-broker) version used to process the data",
            },
            ignore_index=True,
        )

        ztf_candidate = ztf_candidate._append(
            {
                "name": "fink_science_version",
                "type": "string",
                "doc": "Science modules (fink-science) version used to process the data",
            },
            ignore_index=True,
        )

        ztf_cutouts = pd.DataFrame.from_dict(
            [
                {
                    "name": "cutoutScience_stampData",
                    "type": "array",
                    "doc": "2D array from the Science cutout FITS",
                },
            ],
        )
        ztf_cutouts = ztf_cutouts._append(
            {
                "name": "cutoutTemplate_stampData",
                "type": "array",
                "doc": "2D array from the Template cutout FITS",
            },
            ignore_index=True,
        )
        ztf_cutouts = ztf_cutouts._append(
            {
                "name": "cutoutDifference_stampData",
                "type": "array",
                "doc": "2D array from the Difference cutout FITS",
            },
            ignore_index=True,
        )

        # Science modules
        fink_science = pd.DataFrame(
            [
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
                    "name": "mulens",
                    "type": "double",
                    "doc": "Probability score of an alert to be a microlensing event by [LIA](https://github.com/dgodinez77/LIA).",
                },
                {
                    "name": "rf_snia_vs_nonia",
                    "type": "double",
                    "doc": "Probability of an alert to be a SNe Ia using a Random Forest Classifier (binary classification). Higher is better.",
                },
                {
                    "name": "rf_kn_vs_nonkn",
                    "type": "double",
                    "doc": "Probability of an alert to be a Kilonova using a PCA & Random Forest Classifier (binary classification). Higher is better.",
                },
                {
                    "name": "roid",
                    "type": "int",
                    "doc": "Determine if the alert is a potential Solar System object (experimental). 0: likely not SSO, 1: first appearance but likely not SSO, 2: candidate SSO, 3: found in MPC.",
                },
                {
                    "name": "snn_sn_vs_all",
                    "type": "double",
                    "doc": "The probability of an alert to be a SNe vs. anything else (variable stars and other categories in the training) using SuperNNova",
                },
                {
                    "name": "snn_snia_vs_nonia",
                    "type": "double",
                    "doc": "The probability of an alert to be a SN Ia vs. core-collapse SNe using SuperNNova",
                },
                {
                    "name": "anomaly_score",
                    "type": "double",
                    "doc": "Probability of an alert to be anomalous (lower values mean more anomalous observations) based on lc_*",
                },
                {
                    "name": "nalerthist",
                    "type": "int",
                    "doc": "Number of detections contained in each alert (current+history). Upper limits are not taken into account.",
                },
                {
                    "name": "tracklet",
                    "type": "string",
                    "doc": "ID for fast moving objects, typically orbiting around the Earth. Of the format YYYY-MM-DD hh:mm:ss",
                },
                {
                    "name": "lc_features_g",
                    "type": "string",
                    "doc": "Numerous light curve features for the g band (see https://github.com/astrolabsoftware/fink-science/tree/master/fink_science/ztf/ad_features). Stored as string of array.",
                },
                {
                    "name": "lc_features_r",
                    "type": "string",
                    "doc": "Numerous light curve features for the r band (see https://github.com/astrolabsoftware/fink-science/tree/master/fink_science/ztf/ad_features). Stored as string of array.",
                },
                {
                    "name": "jd_first_real_det",
                    "type": "double",
                    "doc": "First variation time at 5 sigma contained in the alert history",
                },
                {
                    "name": "jdstarthist_dt",
                    "type": "double",
                    "doc": "Delta time between `jd_first_real_det` and the first variation time at 3 sigma (`jdstarthist`). If `jdstarthist_dt` > 30 days then the first variation time at 5 sigma is False (accurate for fast transient).",
                },
                {
                    "name": "mag_rate",
                    "type": "double",
                    "doc": "Magnitude rate (mag/day)",
                },
                {
                    "name": "sigma_rate",
                    "type": "double",
                    "doc": "Magnitude rate error estimation (mag/day)",
                },
                {
                    "name": "lower_rate",
                    "type": "double",
                    "doc": "5% percentile of the magnitude rate sampling used for the error computation (`sigma_rate`)",
                },
                {
                    "name": "upper_rate",
                    "type": "double",
                    "doc": "95% percentile of the magnitude rate sampling used for the error computation (`sigma_rate`)",
                },
                {
                    "name": "delta_time",
                    "type": "double",
                    "doc": "Delta time between the the two measurement used for the magnitude rate `mag_rate`",
                },
                {
                    "name": "from_upper",
                    "type": "boolean",
                    "doc": "If True, the magnitude rate `mag_rate` has been computed using the last upper limit and the current measurement.",
                },
                {
                    "name": "tag",
                    "type": "string",
                    "doc": "Quality tag among `valid`, `badquality` (does not satisfy quality cuts), and `upper` (upper limit measurement). Only available if `withupperlim` is set to True.",
                },
                {
                    "name": "tns",
                    "type": "string",
                    "doc": "TNS label, if it exists.",
                },
                {
                    "name": "blazar_stats_m0",
                    "type": "float",
                    "doc": "Feature for characterising CTAO blazar state. Related to low state robustness.",
                },
                {
                    "name": "blazar_stats_m1",
                    "type": "float",
                    "doc": "Feature for characterising CTAO blazar state. Related to low state robustness.",
                },
                {
                    "name": "blazar_stats_m2",
                    "type": "float",
                    "doc": "Feature for characterising CTAO blazar state. Related to low state duration.",
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
                    "name": "is_transient",
                    "type": "boolean",
                    "doc": "True if the alert is considered as pure static transient. See https://zenodo.org/records/4054129.",
                },
                {
                    "name": "slsn_score",
                    "type": "float",
                    "doc": "Superluminous supernovae classification score between 0 and 1. Return -1 if not enough points were available for feature extraction, if the alert is not considered a likely transient, or if the source is less than 30 days old.",
                },
            ],
        )

        fink_derived = pd.DataFrame(
            [
                {
                    "name": "constellation",
                    "type": "string",
                    "doc": "Name of the constellation an alert on the sky is in",
                },
                {
                    "name": "classification",
                    "type": "string",
                    "doc": "Fink inferred classification. See https://api.fink-portal.org/api/v1/classes",
                },
                {
                    "name": "g-r",
                    "type": "double",
                    "doc": "Last g-r measurement for this object.",
                },
                {
                    "name": "sigma(g-r)",
                    "type": "double",
                    "doc": "Error of last g-r measurement for this object.",
                },
                {
                    "name": "rate(g-r)",
                    "type": "double",
                    "doc": "g-r change rate in mag/day (between last and previous g-r measurements).",
                },
                {
                    "name": "sigma(rate(g-r))",
                    "type": "double",
                    "doc": "Error of g-r rate in mag/day (between last and previous g-r measurements).",
                },
                {
                    "name": "rate",
                    "type": "double",
                    "doc": "Brightness change rate in mag/day (between last and previous measurement in this filter).",
                },
                {
                    "name": "sigma(rate)",
                    "type": "double",
                    "doc": "Error of brightness change rate in mag/day (between last and previous measurement in this filter).",
                },
                {
                    "name": "lastdate",
                    "type": "string",
                    "doc": "Human readable datetime for the alert (from the i:jd field).",
                },
                {
                    "name": "firstdate",
                    "type": "string",
                    "doc": "Human readable datetime for the first detection of the object (from the i:jdstarthist field).",
                },
                {
                    "name": "lapse",
                    "type": "double",
                    "doc": "Number of days between first and last detection.",
                },
            ],
        )

        # Sort by name
        ztf_candidate = ztf_candidate.sort_values("name")
        fink_science = fink_science.sort_values("name")
        fink_derived = fink_derived.sort_values("name")

        types = {
            "ZTF original fields (i:)": {
                i: {"type": j, "doc": k}
                for i, j, k in zip(
                    ztf_candidate.name, ztf_candidate.type, ztf_candidate.doc
                )
            },
            "ZTF original cutouts (b:)": {
                i: {"type": j, "doc": k}
                for i, j, k in zip(ztf_cutouts.name, ztf_cutouts.type, ztf_cutouts.doc)
            },
            "Fink science module outputs (d:)": {
                i: {"type": j, "doc": k}
                for i, j, k in zip(
                    fink_science.name, fink_science.type, fink_science.doc
                )
            },
            "Fink on-the-fly added values (v:)": {
                i: {"type": j, "doc": k}
                for i, j, k in zip(
                    fink_derived.name, fink_derived.type, fink_derived.doc
                )
            },
        }

        response = Response(json.dumps(types), 200)
        response.headers.set("Content-Type", "application/json")
        return response
