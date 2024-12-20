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
    payload = {}

    if objectId is not None:
        payload.update({"objectId": objectId})

    if internal_name is not None:
        payload.update({"internal_name": internal_name})

    if internal_name_decoded is not None:
        payload.update({"internal_name_decoded": internal_name_decoded})

    r = requests.post("{}/api/v1/metadata".format(APIURL), json=payload)

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


if __name__ == "__main__":
    """ Execute the test suite """
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
