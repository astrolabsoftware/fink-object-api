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
import urllib.parse
import requests
import pandas as pd

import io
import sys

APIURL = sys.argv[1]

# Implement random name generator
OID = "ZTF21abfmbix"


def get_metadata(objectId=None, internal_name=None, internal_name_encoded=None):
    """Query metadata using GET"""
    if objectId is not None:
        r = requests.get("{}/api/v1/metadata?objectId={}".format(APIURL, objectId))

    if internal_name is not None:
        r = requests.get(
            "{}/api/v1/metadata?internal_name={}".format(APIURL, internal_name)
        )

    if internal_name_encoded is not None:
        r = requests.get(
            "{}/api/v1/metadata?internal_name_encoded={}".format(
                APIURL, internal_name_encoded
            )
        )

    assert r.status_code == 200, r.content

    pdf = pd.read_json(io.BytesIO(r.content))

    return pdf


def test_objectId() -> None:
    """
    Examples
    --------
    >>> test_objectId()
    """
    pdf = get_metadata(objectId="ZTF23aaaatwl")

    assert pdf["d:comments"].values[0] == "coucou", pdf


def test_internal_name() -> None:
    """
    Examples
    --------
    >>> test_internal_name()
    """
    pdf = get_metadata(
        internal_name=urllib.parse.quote_plus("Fink J042203.10+362318.7")
    )
    oid = pdf["i:objectId"].values[0]

    assert oid == "ZTF20aahjjjm", oid

    # Get metadata
    pdf = get_metadata(objectId=oid)
    assert pdf["d:username"].values[0] == "pruzhinskaya", pdf
    assert pdf["d:comments"].values[0] == "Candidate to red dwarf flare, credit", pdf
    assert pdf["d:internal_name_encoded"].values[0] == "FinkJ042203.10+362318.7", pdf


def test_internal_name_encoded() -> None:
    """
    Examples
    --------
    >>> test_internal_name_encoded()
    """
    pdf = get_metadata(
        internal_name_encoded=urllib.parse.quote_plus("FinkJ061603.51+080222.8")
    )

    oid = pdf["i:objectId"].values[0]

    assert oid == "ZTF17aaagtdb", oid

    # Get metadata
    pdf = get_metadata(objectId=oid)
    assert pdf["d:username"].values[0] == "pruzhinskaya", pdf
    assert pdf["d:comments"].values[0] == "Candidate to CV, credit", pdf
    assert pdf["d:internal_name"].values[0] == "Fink J061603.51+080222.8", pdf


def test_get_all() -> None:
    """
    Examples
    --------
    >>> test_get_all()
    """
    pdf = get_metadata(objectId="all")
    assert len(pdf) > 1, pdf


if __name__ == "__main__":
    """ Execute the test suite """
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
