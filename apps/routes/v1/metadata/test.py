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

import io
import sys

APIURL = sys.argv[1]

# Implement random name generator
OID = "ZTF21abfmbix"


def get_metadata(objectId=None, internal_name=None, internal_name_decoded=None):
    """Query metadata"""
    if objectId is not None:
        r = requests.get("{}/api/v1/metadata?objectId={}".format(APIURL, objectId))

    if internal_name is not None:
        r = requests.get(
            "{}/api/v1/metadata?internal_name={}".format(APIURL, internal_name)
        )

    if internal_name_decoded is not None:
        r = requests.get(
            "{}/api/v1/metadata?internal_name_decoded={}".format(APIURL, internal_name)
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

    assert not pdf.empty

    assert pdf["d:comments"].values[0] == "coucou"


if __name__ == "__main__":
    """ Execute the test suite """
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
