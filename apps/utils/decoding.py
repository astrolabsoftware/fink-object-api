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
"""Utilities to decode data from the HBase client"""

from py4j.java_gateway import JavaGateway
import json
import pandas as pd
import numpy as np

from astropy.time import Time
from astropy.coordinates import SkyCoord, get_constellation

from fink_filters.ztf.classification import extract_fink_classification_

from line_profiler import profile

pd.set_option("future.no_silent_downcasting", True)

# For int we use `Int64` due to the presence of NaN
# See https://pandas.pydata.org/pandas-docs/version/1.3/user_guide/integer_na.html
hbase_type_converter = {
    "integer": "Int64",
    "long": "Int64",
    "float": float,
    "double": float,
    "string": str,
    "fits/image": str,
    "boolean": str,
}


@profile
def format_hbase_output(
    hbase_output,
    schema_client,
    group_alerts: bool,
    truncated: bool = False,
    extract_color: bool = True,
    with_constellation: bool = True,
    escape_slash: bool = False,
):
    """Decode the raw HBase payload from Fink/ZTF database into a DataFrame"""
    if len(hbase_output) == 0:
        return pd.DataFrame({})

    # Construct the dataframe
    pdfs = pd.DataFrame.from_dict(
        hbase_to_dict(hbase_output, escape_slash=escape_slash), orient="index"
    )

    # TODO: for not truncated, add a generic mechanism to
    #       add default field value.

    # Tracklet cell contains null if there is nothing
    # and so HBase won't transfer data -- ignoring the column
    if "d:tracklet" not in pdfs.columns and not truncated:
        pdfs["d:tracklet"] = np.zeros(len(pdfs), dtype="U20")

    if "d:tns" not in pdfs.columns and not truncated:
        pdfs["d:tns"] = ""

    if "d:blazar_stats_m0" not in pdfs.columns and not truncated:
        pdfs["d:blazar_stats_m0"] = -1.0
        pdfs["d:blazar_stats_m1"] = -1.0
        pdfs["d:blazar_stats_m2"] = -1.0

    # Remove hbase specific fields
    for _ in ["key:key", "key:time"]:
        if _ in pdfs.columns:
            pdfs = pdfs.drop(columns=_)

    if "d:spicy_name" in pdfs.columns:
        pdfs = pdfs.drop(columns="d:spicy_name")

    # Remove cutouts if their fields are here but empty
    for _ in ["Difference", "Science", "Template"]:
        colname = f"b:cutout{_}_stampData"
        if colname in pdfs.columns and pdfs[colname].to_numpy()[0].startswith(
            "binary:ZTF"
        ):
            pdfs = pdfs.drop(columns=colname)

    # Type conversion
    for col in pdfs.columns:
        pdfs[col] = convert_datatype(
            pdfs[col],
            hbase_type_converter[schema_client.type(col)],
        )

    # Booleans
    pdfs = pdfs.replace(to_replace={"true": True, "false": False})

    # cast 'nan' into `[]` for easier json decoding
    for col in ["d:lc_features_g", "d:lc_features_r"]:
        if col in pdfs.columns:
            pdfs[col] = pdfs[col].replace("nan", "[]")

    pdfs = pdfs.copy()  # Fix Pandas' "DataFrame is highly fragmented" warning

    if not truncated:
        # Fink final classification
        classifications = extract_fink_classification_(
            pdfs["d:cdsxmatch"],
            pdfs["d:roid"],
            pdfs["d:mulens"],
            pdfs["d:snn_snia_vs_nonia"],
            pdfs["d:snn_sn_vs_all"],
            pdfs["d:rf_snia_vs_nonia"],
            pdfs["i:ndethist"],
            pdfs["i:drb"],
            pdfs["i:classtar"],
            pdfs["i:jd"],
            pdfs["i:jdstarthist"],
            pdfs["d:rf_kn_vs_nonkn"],
            pdfs["d:tracklet"],
        )

        pdfs["v:classification"] = classifications.to_numpy()

        if extract_color:
            # Extract color evolution
            pdfs = extract_rate_and_color(pdfs)

        # Human readable time
        pdfs["v:lastdate"] = convert_jd(pdfs["i:jd"])
        pdfs["v:firstdate"] = convert_jd(pdfs["i:jdstarthist"])
        pdfs["v:lapse"] = pdfs["i:jd"] - pdfs["i:jdstarthist"]

        if with_constellation:
            coords = SkyCoord(
                pdfs["i:ra"],
                pdfs["i:dec"],
                unit="deg",
            )
            constellations = get_constellation(coords)
            pdfs["v:constellation"] = constellations

    # Display only the last alert
    if group_alerts and ("i:jd" in pdfs.columns) and ("i:objectId" in pdfs.columns):
        pdfs["i:jd"] = pdfs["i:jd"].astype(float)
        pdfs = pdfs.loc[pdfs.groupby("i:objectId")["i:jd"].idxmax()]

    # sort values by time
    if "i:jd" in pdfs.columns:
        pdfs = pdfs.sort_values("i:jd", ascending=False)

    return pdfs


@profile
def format_lsst_hbase_output(
    hbase_output,
    schema_client,
    group_alerts: bool,
    truncated: bool = False,
    extract_color: bool = True,
    with_constellation: bool = True,
    escape_slash: bool = False,
):
    """Decode the raw HBase payload from Fink/LSST database into a DataFrame"""
    if len(hbase_output) == 0:
        return pd.DataFrame({})

    # Construct the dataframe
    pdfs = pd.DataFrame.from_dict(
        hbase_to_dict(hbase_output, escape_slash=escape_slash), orient="index"
    )

    # Remove hbase specific fields
    for _ in ["key:key", "key:time"]:
        if _ in pdfs.columns:
            pdfs = pdfs.drop(columns=_)

    # Create a dictionary to hold the new columns
    new_columns = {}

    # Use fixed schema
    if truncated:
        cols = pdfs.columns
    else:
        cols = schema_client.columnNames()

    for col in cols:
        if col in pdfs.columns:
            # Type conversion
            new_columns[col] = convert_datatype(
                pdfs[col],
                hbase_type_converter[schema_client.type(col)],
            )
        else:
            # Column is only None so it was not transferred
            # Initialize the column with None and set the correct dtype
            dtype = hbase_type_converter[schema_client.type(col)]
            new_columns[col] = pd.Series(
                [None] * len(pdfs), dtype=dtype, index=pdfs.index
            )

    # Create a new DataFrame with the new columns (overwrite)
    pdfs = pd.DataFrame(new_columns)

    # Booleans
    pdfs = pdfs.replace(to_replace={"true": True, "false": False})

    # cast 'nan' into `[]` for easier json decoding
    for col in ["f:lc_features_g", "f:lc_features_r"]:
        if col in pdfs.columns:
            pdfs[col] = pdfs[col].replace("nan", "[]")

    # Display only the last alert
    if (
        group_alerts
        and ("r:midpointMjdTai" in pdfs.columns)
        and ("r:diaObjectId" in pdfs.columns)
    ):
        pdfs["r:midpointMjdTai"] = pdfs["r:midpointMjdTai"].astype(float)
        pdfs = pdfs.loc[pdfs.groupby("r:diaObjectId")["r:midpointMjdTai"].idxmax()]

    # sort values by time
    if "r:midpointMjdTai" in pdfs.columns:
        pdfs = pdfs.sort_values("r:midpointMjdTai", ascending=False)

    return pdfs


@profile
def hbase_to_dict(hbase_output, escape_slash=False):
    """Optimize hbase output TreeMap for faster conversion to DataFrame"""
    gateway = JavaGateway(auto_convert=True)
    GSONObject = gateway.jvm.com.google.gson.Gson

    # We do bulk export to JSON on Java side to avoid overheads of iterative access
    # and then parse it back to Dict in Python
    if escape_slash:
        hbase_output = str(hbase_output)
    optimized = json.loads(GSONObject().toJson(hbase_output))

    return optimized


def convert_datatype(series: pd.Series, type_: type) -> pd.Series:
    """Convert Series from HBase data with proper type

    Parameters
    ----------
    series: pd.Series
        a column of the DataFrame
    type_: type
        Python built-in type (Int64, int, str, float, bool)
    """
    return series.astype(type_)


@profile
def extract_rate_and_color(pdf: pd.DataFrame, tolerance: float = 0.3):
    """Extract magnitude rates in different filters, color, and color change rate.

    Notes
    -----
    It fills the following fields:
    - v:rate - magnitude change rate for this filter, defined as magnitude difference since previous measurement, divided by time difference
    - v:sigma(rate) - error of previous value, estimated from per-point errors
    - v:g-r - color, defined by subtracting the measurements in g and r filter closer than `tolerance` days. Is assigned to both g and r data points with the same value
    - v:sigma(g-r) - error of previous value, estimated from per-point errors
    - v:rate(g-r) - color change rate, computed using time differences of g band points
    - v:sigma(rate(g-r)) - error of previous value, estimated from per-point errors

    Parameters
    ----------
    pdf: Pandas DataFrame
        DataFrame returned by `format_hbase_output` (see api/api.py)
    tolerance: float
        Maximum delay between g and r data points to be considered for color computation, in days

    Returns
    -------
    pdf: Pandas DataFrame
        Modified original DataFrame with added columns. Original order is not preserved
    """
    pdfs = pdf.sort_values("i:jd")

    def fn(sub):
        """Extract everything relevant on the sub-group corresponding to single object.

        Notes
        -----
        Assumes it is already sorted by time.
        """
        sidx = []

        # Extract magnitude rates separately in different filters
        for fid in [1, 2]:
            idx = sub["i:fid"] == fid

            dmag = sub["i:magpsf"][idx].diff()
            dmagerr = np.hypot(sub["i:sigmapsf"][idx], sub["i:sigmapsf"][idx].shift())
            djd = sub["i:jd"][idx].diff()
            sub.loc[idx, "v:rate"] = dmag / djd
            sub.loc[idx, "v:sigma(rate)"] = dmagerr / djd

            sidx.append(idx)

        if len(sidx) == 2:
            # We have both filters, let's try to also get the color!
            colnames_gr = ["i:jd", "i:magpsf", "i:sigmapsf"]
            gr = pd.merge_asof(
                sub[sidx[0]][colnames_gr],
                sub[sidx[1]][colnames_gr],
                on="i:jd",
                suffixes=("_g", "_r"),
                direction="nearest",
                tolerance=tolerance,
            )
            # It is organized around g band points, r columns are null when unmatched
            gr = gr.loc[~gr.isna()["i:magpsf_r"]]  # Keep only matched rows

            gr["v:g-r"] = gr["i:magpsf_g"] - gr["i:magpsf_r"]
            gr["v:sigma(g-r)"] = np.hypot(gr["i:sigmapsf_g"], gr["i:sigmapsf_r"])

            djd = gr["i:jd"].diff()
            dgr = gr["v:g-r"].diff()
            dgrerr = np.hypot(gr["v:sigma(g-r)"], gr["v:sigma(g-r)"].shift())

            gr["v:rate(g-r)"] = dgr / djd
            gr["v:sigma(rate(g-r))"] = dgrerr / djd

            # Now we may assign these color values also to corresponding r band points
            sub = pd.merge_asof(
                sub,
                gr[
                    [
                        "i:jd",
                        "v:g-r",
                        "v:sigma(g-r)",
                        "v:rate(g-r)",
                        "v:sigma(rate(g-r))",
                    ]
                ],
                direction="nearest",
                tolerance=tolerance,
            )

        return sub

    # Apply the subroutine defined above to individual objects, and merge the table back
    pdfs = pdfs.groupby("i:objectId").apply(fn).droplevel(0)

    return pdfs


def convert_jd(jd, to="iso", format="jd"):
    """Convert Julian Date into ISO date (UTC)."""
    return Time(jd, format=format).to_value(to)
