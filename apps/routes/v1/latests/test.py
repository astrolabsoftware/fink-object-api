# Copyright 2022-2024 AstroLab Software
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

from astropy.io import votable

import io
import sys

APIURL = sys.argv[1]


def classsearch(
    myclass="Early SN Ia candidate",
    n=10,
    trend=None,
    startdate=None,
    stopdate=None,
    output_format="json",
    cols=None,
):
    """Perform a class search in the Science Portal using the Fink REST API"""
    payload = {"class": myclass, "n": n, "output-format": output_format}

    if startdate is not None:
        payload.update({"startdate": startdate, "stopdate": stopdate})

    if cols is not None:
        payload.update({"columns": cols})

    if trend is not None:
        payload.update({"trend": trend})

    r = requests.post("{}/api/v1/latests".format(APIURL), json=payload)

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


def test_simple_classsearch() -> None:
    """
    Examples
    --------
    >>> test_simple_classsearch()
    """
    pdf = classsearch()

    assert not pdf.empty

    assert len(pdf) == 10, len(pdf)

    assert np.all(pdf["v:classification"].to_numpy() == "Early SN Ia candidate")


def test_simbad_classsearch() -> None:
    """
    Examples
    --------
    >>> test_simbad_classsearch()
    """
    pdf = classsearch(myclass="(SIMBAD) QSO")

    assert not pdf.empty

    assert len(pdf) == 10, len(pdf)

    assert np.all(pdf["v:classification"].to_numpy() == "QSO")

    pdf2 = classsearch(myclass="QSO")

    assert pdf.equals(pdf2)


def test_tns_classsearch() -> None:
    """
    Examples
    ---------
    >>> test_tns_classsearch()
    """
    pdf = classsearch(myclass="(TNS) SN Ia")

    assert not pdf.empty


def test_classsearch_and_date() -> None:
    """
    Examples
    --------
    >>> test_classsearch_and_date()
    """
    pdf = classsearch(startdate="2021-11-01", stopdate="2021-12-01")

    assert not pdf.empty

    assert len(pdf) == 10, len(pdf)

    assert np.all(pdf["v:classification"].to_numpy() == "Early SN Ia candidate")

    assert np.all(pdf["v:lastdate"].to_numpy() < "2021-12-01")

    assert np.all(pdf["v:lastdate"].to_numpy() >= "2021-11-01")


def test_classsearch_and_cols_with_sort() -> None:
    """
    Examples
    --------
    >>> test_classsearch_and_cols_with_sort()
    """
    pdf = classsearch(cols="i:jd,i:objectId")

    assert not pdf.empty

    assert len(pdf.columns) == 2, len(pdf.columns)

    assert "i:jd" in pdf.columns
    assert "i:objectId" in pdf.columns
    assert "v:classifation" not in pdf.columns


def test_classsearch_and_cols_without_sort() -> None:
    """
    Examples
    --------
    >>> test_classsearch_and_cols_without_sort()
    """
    pdf = classsearch(cols="i:objectId")

    assert not pdf.empty

    assert len(pdf.columns) == 1, len(pdf.columns)

    assert "i:objectId" in pdf.columns


def test_general_trends() -> None:
    """
    Examples
    --------
    >>> test_general_trends()
    """
    start = "2024-11-03 12:30:00"
    stop = "2024-12-03 12:30:00"
    pdf = classsearch(
        myclass="Early SN Ia candidate",
        startdate=start,
        stopdate=stop,
    )
    pdf_rising = classsearch(
        myclass="Early SN Ia candidate",
        trend="rising",
        startdate=start,
        stopdate=stop,
    )
    pdf_fading = classsearch(
        myclass="Early SN Ia candidate",
        trend="fading",
        startdate=start,
        stopdate=stop,
    )

    assert len(pdf) == len(pdf_rising) + len(pdf_fading), (len(pdf), len(pdf_rising))

    assert np.all(pdf_rising["d:mag_rate"].to_numpy() < 0)
    assert np.all(pdf_fading["d:mag_rate"].to_numpy() > 0)


def test_blazar_trends() -> None:
    """
    Examples
    --------
    >>> test_blazar_trends()
    """
    start = "2025-02-02 12:30:00"
    stop = "2025-03-02 12:30:00"
    pdf_low_state = classsearch(
        myclass="(CTA) Blazar",
        trend="low_state",
        startdate=start,
        stopdate=stop,
    )
    pdf_new_low_state = classsearch(
        myclass="(CTA) Blazar",
        trend="new_low_state",
        startdate=start,
        stopdate=stop,
    )

    assert len(pdf_low_state) == 7, len(pdf_low_state)
    assert len(pdf_new_low_state) == 0  # to be changed later


def test_query_url() -> None:
    """
    Examples
    --------
    >>> test_query_url()
    """
    pdf1 = classsearch()

    url = (
        "{}/api/v1/latests?class=Early SN Ia candidate&n=10&output-format=json".format(
            APIURL
        )
    )
    r = requests.get(url)
    pdf2 = pd.read_json(io.BytesIO(r.content))

    # subset of cols to avoid type issues
    cols = ["i:ra", "i:dec"]

    isclose = np.isclose(pdf1[cols], pdf2[cols])
    assert np.all(isclose)


def test_various_outputs() -> None:
    """
    Examples
    --------
    >>> test_various_outputs()
    """
    pdf1 = classsearch(output_format="json")

    for fmt in ["csv", "parquet", "votable"]:
        pdf2 = classsearch(output_format=fmt)

        # subset of cols to avoid type issues
        cols1 = ["i:ra", "i:dec"]

        # https://docs.astropy.org/en/stable/io/votable/api_exceptions.html#w02-x-attribute-y-is-invalid-must-be-a-standard-xml-id
        cols2 = cols1 if fmt != "votable" else ["i_ra", "i_dec"]

        isclose = np.isclose(pdf1[cols1], pdf2[cols2])
        assert np.all(isclose), fmt


if __name__ == "__main__":
    """ Execute the test suite """
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
