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
import requests
import pandas as pd
import numpy as np

import io
import sys

APIURL = sys.argv[1]

# Implement random name generator
OID = "169298433216610349"


def get_an_object(
    oid="169298433216610349",
    midpointMjdTai=None,
    output_format="json",
    columns="*",
):
    """Query an object from the Science Portal using the Fink REST API"""
    payload = {
        "diaObjectId": oid,
        "columns": columns,
        "output-format": output_format,
    }

    if midpointMjdTai is not None:
        payload.update({"midpointMjdTai": midpointMjdTai})

    r = requests.post("{}/api/v1/sources".format(APIURL), json=payload)

    assert r.status_code == 200, r.content

    if output_format == "json":
        # Format output in a DataFrame
        pdf = pd.read_json(io.BytesIO(r.content))
    elif output_format == "csv":
        pdf = pd.read_csv(io.BytesIO(r.content))
    elif output_format == "parquet":
        pdf = pd.read_parquet(io.BytesIO(r.content))

    return pdf


def test_single_object() -> None:
    """
    Examples
    --------
    >>> test_single_object()
    """
    pdf = get_an_object(oid=OID)

    assert not pdf.empty

def test_single_object_with_date() -> None:
    """
    Examples
    --------
    >>> test_single_object_with_date()
    """
    pdf = get_an_object(oid=OID, midpointMjdTai=60924.3322219485)

    assert len(pdf) == 1, len(pdf)


def test_single_object_csv() -> None:
    """
    Examples
    --------
    >>> test_single_object_csv()
    """
    pdf = get_an_object(oid=OID, output_format="csv")

    assert not pdf.empty


def test_single_object_parquet() -> None:
    """
    Examples
    --------
    >>> test_single_object_parquet()
    """
    pdf = get_an_object(oid=OID, output_format="parquet")

    assert not pdf.empty


def test_column_selection() -> None:
    """
    Examples
    --------
    >>> test_column_selection()
    """
    pdf = get_an_object(oid=OID, columns="i:midpointMjdTai,i:psfFlux")

    assert len(pdf.columns) == 2, "I count {} columns".format(len(pdf.columns))


def test_formatting() -> None:
    """
    Examples
    --------
    >>> test_formatting()
    """
    pdf = get_an_object(oid=OID)

    # stupid python cast...
    assert isinstance(pdf["i:band"].to_numpy()[0], str), type(
        pdf["i:band"].to_numpy()[0]
    )
    assert isinstance(pdf["i:psfFlux"].to_numpy()[0], np.double), type(
        pdf["i:psfFlux"].to_numpy()[0]
    )


def test_misc() -> None:
    """
    Examples
    --------
    >>> test_misc()
    """
    pdf = get_an_object(oid=OID)
    assert np.all(pdf["i:midpointMjdTai"].to_numpy() > 0)


def test_bad_request() -> None:
    """
    Examples
    --------
    >>> test_bad_request()
    """
    pdf = get_an_object(oid="ldfksjflkdsjf")

    assert pdf.empty


def test_multiple_objects() -> None:
    """
    Examples
    --------
    >>> test_multiple_objects()
    """
    OIDS_ = [OID, "169342391073374215"]
    OIDS = ",".join(OIDS_)
    pdf = get_an_object(oid=OIDS)
    assert not pdf.empty, OIDS
    assert "i:diaObjectId" in pdf.columns, pdf.columns

    n_oids = len(np.unique(pdf.groupby("i:diaObjectId").count()["i:ra"]))
    assert n_oids == 2

    n_oids_single = 0
    len_object = 0
    for oid in OIDS_:
        pdf_ = get_an_object(oid=oid)
        n_oid = len(np.unique(pdf_.groupby("i:diaObjectId").count()["i:ra"]))
        n_oids_single += n_oid
        len_object += len(pdf_)

    assert n_oids == n_oids_single, "{} is not equal to {}".format(
        n_oids, n_oids_single
    )
    assert len_object == len(pdf), "{} is not equal to {}".format(len_object, len(pdf))


if __name__ == "__main__":
    """ Execute the test suite """
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
