# Copyright 2024 AstroLab Software
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
from flask import Response
import pandas as pd

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import hbase_to_dict
from apps.utils.utils import extract_configuration

from line_profiler import profile


@profile
def post_metadata(payload: dict) -> Response:
    """Upload metadata in Fink"""
    client = connect_to_hbase_table("ztf.metadata")
    encoded = payload["internal_name"].replace(" ", "")
    client.put(
        payload["objectId"].strip(),
        [
            "d:internal_name:{}".format(payload["internal_name"]),
            f"d:internal_name_encoded:{encoded}",
            "d:comments:{}".format(payload["comments"]),
            "d:username:{}".format(payload["username"]),
        ],
    )
    client.close()

    config = extract_configuration("config.yml")

    return Response(
        "Thanks {} - You can visit {}/{}".format(
            payload["username"],
            config["APIURL"],
            encoded,
        ),
        200,
    )


@profile
def retrieve_metadata(objectId: str) -> pd.DataFrame:
    """Retrieve metadata in Fink given a ZTF object ID"""
    client = connect_to_hbase_table("ztf.metadata")
    if objectId.startswith("ZTF"):
        to_evaluate = f"key:key:{objectId}"
    elif objectId == "all":
        to_evaluate = "key:key:ZTF"
    results = client.scan(
        "",
        to_evaluate,
        "*",
        0,
        False,
        False,
    )
    pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")
    client.close()
    return pdf


@profile
def retrieve_oid(metaname: str, field: str) -> pd.DataFrame:
    """Retrieve a ZTF object ID given metadata in Fink"""
    client = connect_to_hbase_table("ztf.metadata")
    to_evaluate = f"d:{field}:{metaname}:exact"
    results = client.scan(
        "",
        to_evaluate,
        "*",
        0,
        True,
        True,
    )
    pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")
    client.close()

    return pdf
