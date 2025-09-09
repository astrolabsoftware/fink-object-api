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
import pandas as pd

from astropy.time import Time

from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import hbase_to_dict, convert_datatype, hbase_type_converter

from line_profiler import profile


@profile
def get_ssocand(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/ssocand

    Parameters
    ----------
    payload: dict
        See https://api.fink-portal.org

    Return
    ----------
    out: pandas dataframe
    """
    if "ssoCandId" in payload:
        trajectory_id = str(payload["ssoCandId"])
    else:
        trajectory_id = None

    if "maxnumber" in payload:
        maxnumber = payload["maxnumber"]
    else:
        maxnumber = 10000

    payload_name = payload["kind"]

    if payload_name == "orbParams":
        gen_client = connect_to_hbase_table("ztf.orb_cand")

        if trajectory_id is not None:
            to_evaluate = f"key:key:cand_{trajectory_id}"
        else:
            to_evaluate = "key:key:cand_"
    elif payload_name == "lightcurves":
        gen_client = connect_to_hbase_table("ztf.sso_cand")

        if "start_date" in payload:
            start_date = Time(payload["start_date"], format="iso").jd
        else:
            start_date = Time("2019-11-01", format="iso").jd

        if "stop_date" in payload:
            stop_date = Time(payload["stop_date"], format="iso").jd
        else:
            stop_date = Time.now().jd

        gen_client.setRangeScan(True)
        gen_client.setLimit(maxnumber)

        if trajectory_id is not None:
            gen_client.setEvaluation(f"ssoCandId.equals('{trajectory_id}')")

        to_evaluate = f"key:key:{start_date}_,key:key:{stop_date}_"

    results = gen_client.scan(
        "",
        to_evaluate,
        "*",
        0,
        False,
        False,
    )

    schema_client = gen_client.schema()
    gen_client.close()

    if results.isEmpty():
        return pd.DataFrame({})

    # Construct the dataframe
    pdf = pd.DataFrame.from_dict(hbase_to_dict(results), orient="index")

    if "key:time" in pdf.columns:
        pdf = pdf.drop(columns=["key:time"])

    # Type conversion
    for col in pdf.columns:
        pdf[col] = convert_datatype(
            pdf[col],
            hbase_type_converter[schema_client.type(col)],
        )

    return pdf
