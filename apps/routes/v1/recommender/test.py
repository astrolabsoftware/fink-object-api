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

import io
import sys

APIURL = sys.argv[1]

# Implement random name generator
OID = "ZTF21abfmbix"


def get_recommendation(
    oid="ZTF21abfmbix",
    classifier="FINK_CLASS",
    nobjects=10,
    output_format="json",
):
    """Get recommendation from the classification graph using the Fink REST API"""
    payload = {
        "objectId": oid,
        "classifier": classifier,
        "n": nobjects,
        "output-format": output_format,
    }

    r = requests.post("{}/api/v1/recommender".format(APIURL), json=payload)

    assert r.status_code == 200, r.content

    if output_format == "json":
        # Format output in a DataFrame
        pdf = pd.read_json(io.BytesIO(r.content))
    elif output_format == "csv":
        pdf = pd.read_csv(io.BytesIO(r.content))
    elif output_format == "parquet":
        pdf = pd.read_parquet(io.BytesIO(r.content))

    return pdf


def test_object() -> None:
    """
    Examples
    --------
    >>> test_object()
    """
    pdf = get_recommendation(oid=OID)

    assert not pdf.empty

    assert len(pdf) == 10, len(pdf)


def test_stability() -> None:
    """
    Examples
    --------
    >>> test_n_object()
    """
    pdf1 = get_recommendation(oid=OID, n=20)
    pdf2 = get_recommendation(oid=OID, n=10)

    assert len(pdf1) == 20, len(pdf1)
    assert len(pdf2) == 10, len(pdf2)

    # first 10 must be the same
    assert pdf1.head(10).equals(pdf2), (pdf1, pdf2)


if __name__ == "__main__":
    """ Execute the test suite """
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
