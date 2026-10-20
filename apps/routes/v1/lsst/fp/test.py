# Copyright 2026 AstroLab Software
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
import io
import sys

import numpy as np
import pandas as pd
import requests

APIURL = sys.argv[1]

OID = "313875415113400364"
COLUMNS = "r:midpointMjdTai,r:psfFlux,r:band,r:diaObjectId"


def get_fp(oid=OID, columns=COLUMNS, output_format="json"):
    """Query forced photometry using the Fink REST API"""
    payload = {
        "diaObjectId": oid,
        "columns": columns,
        "output-format": output_format,
    }

    r = requests.post(f"{APIURL}/api/v1/fp", json=payload)

    assert r.status_code == 200, r.content

    if output_format == "json":
        pdf = pd.read_json(io.BytesIO(r.content))
    else:
        pdf = pd.read_parquet(io.BytesIO(r.content))

    return pdf


def test_single_object() -> None:
    """
    Examples
    --------
    >>> test_single_object()
    """
    pdf = get_fp()

    assert not pdf.empty

    for col in COLUMNS.split(","):
        assert col in pdf.columns, [col, pdf.columns]


def test_single_object_parquet() -> None:
    """
    Examples
    --------
    >>> test_single_object_parquet()
    """
    pdf = get_fp(output_format="parquet")

    assert not pdf.empty


def test_column_selection() -> None:
    """
    Examples
    --------
    >>> test_column_selection()
    """
    pdf = get_fp(columns="r:midpointMjdTai,r:psfFlux")

    assert len(pdf.columns) == 2, f"I count {len(pdf.columns)} columns {pdf.columns}"


def test_unknown_object() -> None:
    """
    Examples
    --------
    >>> test_unknown_object()
    """
    pdf = get_fp(oid="ldfksjflkdsjf")

    assert pdf.empty


def test_bad_request() -> None:
    """
    Examples
    --------
    >>> test_bad_request()
    """
    r = requests.post(f"{APIURL}/api/v1/fp", json={"columns": COLUMNS})

    assert r.status_code == 400, r.content


def test_multiple_objects() -> None:
    """
    Examples
    --------
    >>> test_multiple_objects()
    """
    OIDS_ = [OID, "314003014107006318"]
    pdf = get_fp(oid=",".join(OIDS_))

    oids = np.unique(pdf["r:diaObjectId"])
    assert len(oids) == 2, oids

    len_object = sum(len(get_fp(oid=oid)) for oid in OIDS_)
    assert len_object == len(pdf), f"{len_object} is not equal to {len(pdf)}"


if __name__ == "__main__":
    """ Execute the test suite """
    import doctest
    import sys

    sys.exit(doctest.testmod()[0])
