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
import requests
import numpy as np
import logging

import sys

APIURL = sys.argv[1]
_LOG = logging.getLogger(__name__)


def get_fields_per_family():
    """Get fields exposed in /api/v1/schema"""
    r = requests.post(
        "{}/api/v1/schema".format(APIURL), json={"endpoint": "/api/v1/sources"}
    )

    assert r.status_code == 200, r.content

    schema = r.json()

    categories = schema.keys()

    dic = {}
    for i in ["r:", "f:"]:
        for category in categories:
            if i in category:
                dic[i] = list(schema[category].keys())

    return dic


def get_fields_from_schema(schema_dic):
    """Get fields exposed in /api/v1/schema"""
    categories = schema_dic.keys()

    out = []
    for i in ["r:", "f:"]:
        for category in categories:
            if i in category:
                tmp = [i + k for k in list(schema_dic[category].keys())]
                out.append(tmp)
    if len(out) > 1:
        return np.concatenate(out)
    else:
        return out[0]


def check_schema_endpoint():
    """Check there is not holes in the schema

    Examples
    --------
    >>> check_schema_endpoint()
    """
    diaobjectid = "313875415113400364"
    endpoints = {
        "/api/v1/sources": {"diaObjectId": diaobjectid},
        "/api/v1/objects": {"diaObjectId": diaobjectid},
        # "/api/v1/sso": {"name_or_d": ""},
        "/api/v1/conesearch": {
            "ra": "10 02 38.65",
            "dec": "+00 51 02.6",
            "radius": "5",
        },
        "/api/v1/tags": {"tag": "in_tns"},
        "/api/v1/statistics": {"date": "20260129"},
    }

    for endpoint, payload in endpoints.items():
        _LOG.warning(endpoint)
        r_schema = requests.post(
            "{}/api/v1/schema".format(APIURL), json={"endpoint": endpoint}
        )
        r_data = requests.post("{}{}".format(APIURL, endpoint), json=payload)

        if r_data.status_code != 200:
            _LOG.error(r_data.content)
            raise r_data.raise_for_status()
        if r_schema.status_code != 200:
            _LOG.error(r_schema.content)
            raise r_schema.raise_for_status()
        schema_fields = get_fields_from_schema(r_schema.json())

        if len(r_data.json()) == 0:
            _LOG.error(
                "Data is empty for endpoint {} with payload {}".format(
                    endpoint, payload
                )
            )
            raise ValueError
        data_fields = list(r_data.json()[0].keys())

        # Expected non-null as not all alerts have all fields
        # not_in_alert = [i for i in schema_fields if i not in data_fields]

        # Should be empty!
        not_in_schema = [i for i in data_fields if i not in schema_fields]

        if len(not_in_schema) >= 1:
            allowed_fields = ["f:pixel1024", "v:separation_degree", "r:salt"]
            is_allowed = [i in allowed_fields for i in not_in_schema]
            assert np.sum(is_allowed) == len(not_in_schema), (endpoint, not_in_schema)


if __name__ == "__main__":
    """ Execute the test suite """
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
