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


def get_data_from_tags(
    tag="in_tns",
    nalert=10,
    startdate=None,
    stopdate=None,
    columns="*",
    output_format="json",
):
    """Query data based on tags from the Science Portal using the Fink REST API"""
    payload = {
        "tag": tag,
        "n": nalert,
        "columns": columns,
        "output-format": output_format,
    }

    if startdate is not None:
        payload.update({"startdate": startdate})

    if stopdate is not None:
        payload.update({"stopdate": stopdate})

    r = requests.post(f"{APIURL}/api/v1/tags", json=payload)

    assert r.status_code == 200, r.content

    if output_format == "json":
        # Format output in a DataFrame
        pdf = pd.read_json(io.BytesIO(r.content))
    elif output_format == "csv":
        pdf = pd.read_csv(io.BytesIO(r.content), index_col=False)
    elif output_format == "parquet":
        pdf = pd.read_parquet(io.BytesIO(r.content))

    return pdf


def test_retrieve_data_from_tag() -> None:
    """
    Examples
    --------
    >>> test_retrieve_data_from_tag()
    """
    pdf = get_data_from_tags()

    assert not pdf.empty
    assert len(pdf) == 10, len(pdf)


def test_various_output() -> None:
    """
    Examples
    --------
    >>> test_various_output()
    """
    pdfj = get_data_from_tags(output_format="json")
    pdfc = get_data_from_tags(output_format="csv")
    pdfp = get_data_from_tags(output_format="parquet").reset_index()

    assert pdfj["r:diaSourceId"].equals(pdfc["r:diaSourceId"]), (
        pdfj["r:diaSourceId"],
        pdfc["r:diaSourceId"],
    )

    # Parquet uses Int64 and not int64
    assert (
        pdfj["r:diaSourceId"].astype(pd.Int64Dtype()).equals(pdfp["r:diaSourceId"])
    ), (pdfj["r:diaSourceId"], pdfp["r:diaSourceId"])


def test_various_n() -> None:
    """
    Examples
    --------
    >>> test_various_n()
    """
    for n in [1, 10, 50]:
        pdf = get_data_from_tags(nalert=n)
        assert len(pdf) == n, len(pdf)


def test_column_selection() -> None:
    """
    Examples
    --------
    >>> test_column_selection()
    """
    pdf = get_data_from_tags(columns="r:midpointMjdTai,r:psfFlux")

    assert len(pdf.columns) == 2, f"I count {len(pdf.columns)} columns"


def test_bad_request() -> None:
    """
    Examples
    --------
    >>> test_bad_request()
    """
    try:
        get_data_from_tags(tag="ldfksjflkdsjf")
    except AssertionError:
        pass


def test_tags_retrieval() -> None:
    """
    Examples
    --------
    >>> test_tags_retrieval()
    """
    r = requests.get(f"{APIURL}/api/v1/tags")

    assert r.status_code == 200, r.content

    tags = r.json()

    assert "in_tns" in tags, tags

    # atag = tags["in_tns"]

    # assert "description" in atag, atag
    # assert "API support" in atag, atag


if __name__ == "__main__":
    """ Execute the test suite """
    import doctest
    import sys

    sys.exit(doctest.testmod()[0])
