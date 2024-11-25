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
from numpy import array as nparray
from apps.utils.utils import download_cutout
from apps.utils.client import connect_to_hbase_table
from apps.utils.decoding import format_hbase_output, hbase_to_dict


def extract_object_data(payload: dict) -> pd.DataFrame:
    """Extract data returned by HBase and format it in a Pandas dataframe

    Data is from /api/v1/objects

    Parameters
    ----------
    payload: dict
        See https://fink-portal.org/api/v1/objects

    Return
    ----------
    out: pandas dataframe
    """
    if "columns" in payload:
        cols = payload["columns"].replace(" ", "")
    else:
        cols = "*"

    if "," in payload["objectId"]:
        # multi-objects search
        splitids = payload["objectId"].split(",")
        objectids = [f"key:key:{i.strip()}" for i in splitids]
    else:
        # single object search
        objectids = ["key:key:{}".format(payload["objectId"])]

    if "withcutouts" in payload and str(payload["withcutouts"]) == "True":
        withcutouts = True
    else:
        withcutouts = False

    if "withupperlim" in payload and str(payload["withupperlim"]) == "True":
        withupperlim = True
    else:
        withupperlim = False

    if cols == "*":
        truncated = False
    else:
        truncated = True

    client = connect_to_hbase_table("ztf")

    # Get data from the main table
    results = {}
    for to_evaluate in objectids:
        result = client.scan(
            "",
            to_evaluate,
            cols,
            0,
            True,
            True,
        )
        results.update(result)

    schema_client = client.schema()

    pdf = format_hbase_output(
        results,
        schema_client,
        group_alerts=False,
        truncated=truncated,
    )

    if withcutouts:
        # Default `None` returns all 3 cutouts
        cutout_kind = payload.get("cutout-kind", "All")

        if cutout_kind == "All":
            cols = [
                "b:cutoutScience_stampData",
                "b:cutoutTemplate_stampData",
                "b:cutoutDifference_stampData",
            ]
            pdf[cols] = pdf[["i:objectId", "i:candid"]].apply(
                lambda x: pd.Series(download_cutout(x.iloc[0], x.iloc[1], cutout_kind)),
                axis=1,
            )
        else:
            colname = "b:cutout{}_stampData".format(cutout_kind)
            pdf[colname] = pdf[["i:objectId", "i:candid"]].apply(
                lambda x: pd.Series(
                    [download_cutout(x.iloc[0], x.iloc[1], cutout_kind)]
                ),
                axis=1,
            )

    if withupperlim:
        clientU = connect_to_hbase_table("ztf.upper")
        # upper limits
        resultsU = {}
        for to_evaluate in objectids:
            resultU = clientU.scan(
                "",
                to_evaluate,
                "*",
                0,
                False,
                False,
            )
            resultsU.update(resultU)

        # bad quality
        clientUV = connect_to_hbase_table("ztf.uppervalid")
        resultsUP = {}
        for to_evaluate in objectids:
            resultUP = clientUV.scan(
                "",
                to_evaluate,
                "*",
                0,
                False,
                False,
            )
            resultsUP.update(resultUP)

        pdfU = pd.DataFrame.from_dict(hbase_to_dict(resultsU), orient="index")
        pdfUP = pd.DataFrame.from_dict(hbase_to_dict(resultsUP), orient="index")

        pdf["d:tag"] = "valid"
        pdfU["d:tag"] = "upperlim"
        pdfUP["d:tag"] = "badquality"

        if "i:jd" in pdfUP.columns:
            # workaround -- see https://github.com/astrolabsoftware/fink-science-portal/issues/216
            mask = nparray(
                [
                    False if float(i) in pdf["i:jd"].to_numpy() else True
                    for i in pdfUP["i:jd"].to_numpy()
                ]
            )
            pdfUP = pdfUP[mask]

        # Hacky way to avoid converting concatenated column to float
        pdfU["i:candid"] = -1  # None
        pdfUP["i:candid"] = -1  # None

        pdf_ = pd.concat((pdf, pdfU, pdfUP), axis=0)

        # replace
        if "i:jd" in pdf_.columns:
            pdf_["i:jd"] = pdf_["i:jd"].astype(float)
            pdf = pdf_.sort_values("i:jd", ascending=False)
        else:
            pdf = pdf_

        clientU.close()
        clientUV.close()

    client.close()

    return pdf
