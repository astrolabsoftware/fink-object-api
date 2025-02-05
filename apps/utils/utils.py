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
"""Various utilities"""

import io
import json
import yaml
import requests
import logging

from flask import Response

from astropy.table import Table
from astropy.io import votable
from astropy.time import Time

import numpy as np

import rocks

from line_profiler import profile


def extract_configuration(filename):
    """Extract user defined configuration

    Parameters
    ----------
    filename: str
        Full path to the `config.yml` file.

    Returns
    -------
    out: dict
        Dictionary with user defined values.
    """
    config = yaml.load(open("config.yml"), yaml.Loader)
    if config["HOST"].endswith(".org"):
        config["APIURL"] = "https://" + config["HOST"]
    else:
        config["APIURL"] = "http://" + config["HOST"] + ":" + str(config["PORT"])
    return config


@profile
def download_cutout(objectId, candid, kind):
    """ """
    config = extract_configuration("config.yml")

    r = requests.post(
        "{}/api/v1/cutouts".format(config["APIURL"]),
        json={
            "objectId": objectId,
            "candid": candid,
            "kind": kind,
            "output-format": "array",
        },
    )
    if r.status_code == 200:
        data = json.loads(r.content)
    else:
        # TODO: different return based on `kind`?
        logging.warning(
            "Cutout retrieval failed with status {}: {}".format(r.status_code, r.text)
        )
        return []

    if kind != "All":
        return data["b:cutout{}_stampData".format(kind)]
    else:
        return [
            data["b:cutout{}_stampData".format(k)]
            for k in ["Science", "Template", "Difference"]
        ]


def check_args(args: list, payload: dict) -> dict:
    """Check all required arguments have been supplied

    Parameters
    ----------
    """
    required_args = [k for k in args if args[k].required is True]
    for required_arg in required_args:
        if required_arg not in payload:
            rep = {
                "status": "error",
                "text": f"A value for `{required_arg}` is required. See https://api.fink-portal.org \n",
            }
            return rep
    return {"status": "ok"}


def send_tabular_data(pdf, output_format):
    """Send tabular data over HTTP

    Parameters
    ----------
    pdf: pd.DataFrame
        Pandas DataFrame with data to be sent
    output_format: str
        Output format: json, csv, votable, parquet.

    Returns
    -------
    out: Any
        Depends on the `output_format` chosen. In
        case of error, returns `Response` object.
    """
    if output_format == "json":
        response = Response(pdf.to_json(orient="records"), 200)
        response.headers.set("Content-Type", "application/json")
        return response
    elif output_format == "csv":
        # TODO: set header?
        response = Response(pdf.to_csv(index=False), 200)
        response.headers.set("Content-Type", "application/csv")
        return response
    elif output_format == "votable":
        f = io.BytesIO()
        table = Table.from_pandas(pdf)
        vt = votable.from_table(table)
        votable.writeto(vt, f)
        f.seek(0)
        response = Response(f.read(), 200)
        response.headers.set("Content-Type", "text/xml")
        return response
    elif output_format == "parquet":
        f = io.BytesIO()
        pdf.to_parquet(f)
        f.seek(0)
        response = Response(f.read(), 200)
        response.headers.set("Content-Type", "parquet")
        return response

    rep = {
        "status": "error",
        "text": f"Output format `{output_format}` is not supported. Choose among json, csv, votable, or parquet\n",
    }
    return Response(str(rep), 400)


def isoify_time(t):
    """Return time in ISO format

    Parameters
    ----------
    t: Any
        Input time in jd, mjd, or ISOT

    Returns
    -------
    out: str
        Time in ISO format
    """
    try:
        tt = Time(t)
    except ValueError:
        ft = float(t)
        if ft // 2400000:
            tt = Time(ft, format="jd")
        else:
            tt = Time(ft, format="mjd")
    return tt.iso


@profile
def resolve_sso_name_to_ssnamenr(sso_name):
    """Find corresponding ZTF ssnamenr from user input

    Parameters
    ----------
    sso_name: str
        SSO name or number

    Returns
    -------
    out: list of str
        List of corresponding ZTF ssnamenr
    """
    config = extract_configuration("config.yml")

    # search all ssnamenr corresponding quaero -> ssnamenr
    r = requests.post(
        "{}/api/v1/resolver".format(config["APIURL"]),
        json={"resolver": "ssodnet", "name": sso_name, "nmax": 1},
    )
    if r.status_code != 200:
        return []

    ssnamenrs = np.unique([i["i:ssnamenr"] for i in r.json()])

    return ssnamenrs


@profile
def resolve_sso_name(sso_name):
    """Find corresponding UAI name and number using quaero

    Parameters
    ----------
    sso_name: str
        SSO name or number

    Returns
    -------
    name: str
        UAI name. NaN if does not exist.
    number: str
        UAI number. NaN if does not exist.
    """
    sso_name, sso_number = rocks.identify(sso_name)
    return sso_name, sso_number
