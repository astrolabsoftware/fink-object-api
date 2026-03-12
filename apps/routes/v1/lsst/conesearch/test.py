# Copyright 2025-2026 AstroLab Software
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
import numpy as np

from astropy.coordinates import SkyCoord
from astropy.io import votable

import io
import sys

APIURL = sys.argv[1]

RA0 = 53.2514
DEC0 = -27.861


def conesearch(
    ra=RA0,
    dec=DEC0,
    radius=5.0,
    startdate=None,
    stopdate=None,
    window=None,
    columns=None,
    output_format="json",
):
    """Perform a conesearch in the Science Portal using the Fink REST API"""
    payload = {"ra": ra, "dec": dec, "radius": radius, "output-format": output_format}

    if startdate is not None:
        payload.update({"startdate": startdate, "window": window})
    if window is not None:
        payload.update({"window": window})
    if stopdate is not None:
        payload.update({"stopdate": stopdate})
    if columns is not None:
        payload.update({"columns": columns})

    r = requests.post("{}/api/v1/conesearch".format(APIURL), json=payload)

    assert r.status_code == 200, r.content

    if output_format == "json":
        # Format output in a DataFrame
        pdf = pd.read_json(io.BytesIO(r.content))
    elif output_format == "csv":
        pdf = pd.read_csv(io.BytesIO(r.content))
    elif output_format == "parquet":
        pdf = pd.read_parquet(io.BytesIO(r.content))
    elif output_format == "votable":
        vt = votable.parse(io.BytesIO(r.content))
        pdf = vt.get_first_table().to_table().to_pandas()

    return pdf


def test_simple_conesearch() -> None:
    """
    Examples
    --------
    >>> test_simple_conesearch()
    """
    radius = 5
    pdf = conesearch(ra=RA0, dec=DEC0, radius=radius)

    # Not empty
    assert not pdf.empty

    # One object found
    assert len(pdf) == 1

    # Check the candidate is found no further away than the radius
    coord0 = SkyCoord(RA0, DEC0, unit="deg")
    coord1 = SkyCoord(pdf["r:ra"].to_numpy()[0], pdf["r:dec"].to_numpy()[0], unit="deg")

    sep = coord0.separation(coord1).degree * 3600

    assert sep <= radius, sep


def test_conesearch_with_dates() -> None:
    """
    Examples
    --------
    >>> test_conesearch_with_dates()
    """
    # within 10'', two objects
    pdf1 = conesearch(
        radius=10.0,
    )

    # Filtering the date leaves one object
    pdf2 = conesearch(
        startdate="2026-01-10 10:00:00",
        window="1",
        radius=10.0,
    )

    # Filtering the date leaves one object
    pdf3 = conesearch(
        startdate="2026-01-10 10:00:00",
        stopdate="2030-01-12 10:00:00",
        radius=10.0,
    )

    # object(s) found
    assert len(pdf1) == 2
    assert len(pdf2) == 1
    assert len(pdf3) == 1


def test_bad_radius_conesearch() -> None:
    """
    Examples
    --------
    >>> test_bad_radius_conesearch()
    """
    payload = {
        "ra": RA0,
        "dec": DEC0,
        "radius": 36000,
        "output_format": "json",
    }

    r = requests.post("{}/api/v1/conesearch".format(APIURL), json=payload)

    msg = {
        "status": "error",
        "text": "`radius` cannot be bigger than 18,000 arcseconds (5 degrees).\n",
    }
    assert r.text == str(msg), r.text


def test_conesearch_with_cols() -> None:
    """
    Examples
    --------
    >>> test_conesearch_with_cols()
    """
    pdf = conesearch(
        columns="r:diaObjectId",
    )

    assert not pdf.empty, pdf

    # specified fields, plus mandatory i:ra,i:dec, plus computed v:separation
    assert len(pdf.columns) == 4, "I count {} columns".format(len(pdf.columns))


def test_bad_dates() -> None:
    """
    Examples
    --------
    >>> test_bad_dates()
    """
    payload = {
        "ra": RA0,
        "dec": DEC0,
        "radius": 10,
        "startdate": "2026-01-10 10:00:00",
        "columns": "r:diaObjectId",
        "output_format": "json",
    }

    r = requests.post("{}/api/v1/conesearch".format(APIURL), json=payload)

    msg = {
        "status": "error",
        "text": "You need to specify f:firstDiaSourceMjdTaiFink in the columns to filter on dates.\n",
    }
    assert r.text == str(msg), r.text


def test_empty_conesearch() -> None:
    """
    Examples
    --------
    >>> test_empty_conesearch()
    """
    pdf = conesearch(ra=0, dec=80, radius=1)

    assert pdf.empty


def test_coordinates() -> None:
    """
    Examples
    --------
    >>> test_coordinates()
    """
    coords = [
        ["03h33m00.33s", "-27d51m39.74s"],
        ["03 33 00.33", "-27 51 39.74"],
        ["03:33:00.33", "-27:51:39.74"],
    ]
    pdf0 = conesearch(ra=RA0, dec=DEC0, columns="r:diaObjectId")
    for ra, dec in coords:
        pdf = conesearch(ra=RA0, dec=DEC0, columns="r:diaObjectId")
        assert pdf.equals(pdf0)


def test_bad_request() -> None:
    """
    Examples
    --------
    >>> test_bad_request()
    """
    payload = {"ra": "kfdlkj", "dec": "lkfdjf", "radius": 5, "output_format": "json"}

    r = requests.post("{}/api/v1/conesearch".format(APIURL), json=payload)

    msg = {
        "status": "error",
        "text": ValueError("Invalid character at col 0 parsing angle 'kfdlkj'"),
    }
    assert r.text == str(msg), r.text


def test_various_outputs() -> None:
    """
    Examples
    --------
    >>> test_various_outputs()
    """
    pdf1 = conesearch(output_format="json")

    for fmt in ["csv", "parquet", "votable"]:
        pdf2 = conesearch(output_format=fmt)

        # subset of cols to avoid type issues
        cols1 = ["r:ra", "r:dec"]

        # https://docs.astropy.org/en/stable/io/votable/api_exceptions.html#w02-x-attribute-y-is-invalid-must-be-a-standard-xml-id
        cols2 = cols1 if fmt != "votable" else ["r_ra", "r_dec"]

        isclose = np.isclose(pdf1[cols1], pdf2[cols2])
        assert np.all(isclose), fmt


if __name__ == "__main__":
    """ Execute the test suite """
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
