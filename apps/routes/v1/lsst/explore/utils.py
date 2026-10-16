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
from collections import defaultdict
import pandas as pd
from flask import Response
from line_profiler import profile

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_lsst_hbase_output


def extract_classes():
    """Extract all classes from the table

    Returns
    -------
    classes: list of str
        List of classes
    lastseen: list of int
        int(MJD) for the emission date of last time each class was seen
    """
    # FIXME: release the schema_name once all tables are updated
    client = connect_to_hbase_table("rubin.explore", schema_name="schema_5.0rc0_9.5.0")

    client.setLimit(10000)

    results = client.scan(
        "",
        "",
        "r:midpointMjdTai",
        0,
        False,
        False,
    )
    client.close()

    # Group by prefix and track the maximum value
    grouped = defaultdict(lambda: {"max_value": -float("inf"), "keys": []})

    for key, inner_dict in results.items():
        # Extract prefix (everything before the last underscore)
        prefix = key.rsplit("_", 1)[0]
        value = float(inner_dict["r:midpointMjdTai"])

        # Update if this value is higher
        if value > grouped[prefix]["max_value"]:
            grouped[prefix]["max_value"] = value
            grouped[prefix]["keys"] = [key]
        elif value == grouped[prefix]["max_value"]:
            grouped[prefix]["keys"].append(key)

    result = {prefix: data["max_value"] for prefix, data in grouped.items()}
    return result


@profile
def extract_object_data(payload: dict, return_raw: bool = False) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/explore

    Parameters
    ----------
    payload: dict
        See https://api.lsst.fink-portal.org
    return_raw: bool
        If True, return the HBase output, else pandas DataFrame.
        Default is False.

    Return
    ----------
    out: pandas dataframe
    """
    # Get the class
    class_ = payload["class"]

    # Check the class exists
    classes = extract_classes()
    if class_ not in classes:
        msg = f"""
        {class_} is not a valid class. Here is the list of classes:
        {list(classes.keys())}
        And you can always retrieve available classes at https://api.lsst.fink-portal.org/api/v1/explore
        """
        return Response(msg, 400)

    if "columns" in payload:
        cols = payload["columns"].replace(" ", "")
    else:
        cols = "*"

    if cols == "*":
        truncated = False
    else:
        truncated = True

    # FIXME: release the schema_name once all tables are updated
    client = connect_to_hbase_table("rubin.explore", schema_name="schema_5.0rc0_9.5.0")

    client.setLimit(10000)

    is_sso = False
    if class_.startswith("SSO"):
        is_sso = True

    if is_sso:
        main_id = "r:packed_primary_provisional_designation"
    else:
        main_id = "r:diaObjectId"
    oids = client.scan(
        "",
        f"key:key:{class_}",
        "{},r:midpointMjdTai".format(main_id),
        0,
        False,
        False,
    )
    client.close()

    if is_sso:
        # Get data from the main SSO table
        client = connect_to_hbase_table("rubin.diaSource_sso")
        results = {}
        for v in oids.values():
            oid = v[main_id]
            salt = oid[1:3]
            result = client.scan(
                "",
                f"key:key:{salt}_{oid}_",
                cols,
                0,
                True,
                True,
            )
            results.update(result)

        schema_client = client.schema()

    else:
        client = connect_to_hbase_table("rubin.diaSource_static")

        results = {}
        for v in oids.values():
            oid = v[main_id]
            mjd = v["r:midpointMjdTai"]

            salt = oid[-3:]

            key = "{}_{}".format(oid, mjd)
            to_evaluate = f"key:key:{salt}_{key}"

            result = client.scan(
                "",
                to_evaluate,
                cols,
                0,
                False,
                False,
            )
            results.update(result)

        schema_client = client.schema()

    client.close()

    pdf = format_lsst_hbase_output(
        results,
        schema_client,
        group_alerts=False,
        truncated=truncated,
    )

    return pdf
