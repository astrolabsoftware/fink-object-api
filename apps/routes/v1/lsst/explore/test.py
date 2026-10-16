# Copyright 2026 AstroLab Software
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
import io
import sys

import pandas as pd
import requests

APIURL = sys.argv[1]


def get_data_from_explore(
    class_="SIMBAD/EB*",
    columns="*",
    output_format="json",
):
    """Query data based on class from the Science Portal using the Fink REST API"""
    payload = {
        "class": class_,
        "columns": columns,
        "output-format": output_format,
    }

    r = requests.post(f"{APIURL}/api/v1/explore", json=payload)

    assert r.status_code == 200, r.content

    if output_format == "json":
        # Format output in a DataFrame
        pdf = pd.read_json(io.BytesIO(r.content))
    elif output_format == "csv":
        pdf = pd.read_csv(io.BytesIO(r.content), index_col=False)
    elif output_format == "parquet":
        pdf = pd.read_parquet(io.BytesIO(r.content))

    return pdf


def test_retrieve_data_from_class() -> None:
    """
    Examples
    --------
    >>> test_retrieve_data_from_class()
    """
    pdf = get_data_from_explore()

    assert not pdf.empty
    assert len(pdf) == 10, len(pdf)

    pdf = get_data_from_explore(class_="SSO/MPC")

    assert not pdf.empty


def test_various_output() -> None:
    """
    Examples
    --------
    >>> test_various_output()
    """
    pdfj = get_data_from_explore(output_format="json")
    pdfc = get_data_from_explore(output_format="csv")
    pdfp = get_data_from_explore(output_format="parquet").reset_index()

    assert pdfj["r:diaObjectId"].equals(pdfc["r:diaObjectId"]), (
        pdfj["r:diaObjectId"],
        pdfc["r:diaObjectId"],
    )

    # Parquet uses Int64 and not int64
    assert (
        pdfj["r:diaObjectId"].astype(pd.Int64Dtype()).equals(pdfp["r:diaObjectId"])
    ), (pdfj["r:diaObjectId"], pdfp["r:diaObjectId"])


def test_column_selection() -> None:
    """
    Examples
    --------
    >>> test_column_selection()
    """
    pdf = get_data_from_explore(columns="r:midpointMjdTai")

    assert len(pdf.columns) == 1, f"I count {len(pdf.columns)} columns"


def test_bad_request() -> None:
    """
    Examples
    --------
    >>> test_bad_request()
    """
    try:
        get_data_from_explore(class_="fsdlkfl")
    except AssertionError:
        pass


def test_classes_retrieval() -> None:
    """
    Examples
    --------
    >>> test_classes_retrieval()
    """
    r = requests.get(f"{APIURL}/api/v1/explore")

    assert r.status_code == 200, r.content

    classes = r.json()

    assert "SIMBAD/EB*" in classes, classes
    assert "TNS/SN Ia" in classes, classes
    assert "VSX/EB" in classes, classes


if __name__ == "__main__":
    """ Execute the test suite """
    import doctest
    import sys

    sys.exit(doctest.testmod()[0])
