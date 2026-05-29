# Copyright 2022-2025 AstroLab Software
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
import numpy as np

import io
import sys

APIURL = sys.argv[1]


def ssosearch(
    n_or_d="J96T28C",
    withEphem=False,
    columns="*",
    output_format="json",
    expected_status=200,
):
    """Perform a sso search in the Science Portal using the Fink REST API"""
    payload = {
        "n_or_d": n_or_d,
        "withEphem": withEphem,
        "columns": columns,
        "output-format": output_format,
    }

    r = requests.post("{}/api/v1/sso".format(APIURL), json=payload)

    assert r.status_code == expected_status, r.content

    if r.status_code == 200:
        if output_format == "json":
            # Format output in a DataFrame
            pdf = pd.read_json(io.BytesIO(r.content))
        elif output_format == "csv":
            pdf = pd.read_csv(io.BytesIO(r.content))
        elif output_format == "parquet":
            pdf = pd.read_parquet(io.BytesIO(r.content))
    else:
        pdf = pd.DataFrame()

    return pdf


def test_simple_ssosearch() -> None:
    """
    Examples
    --------
    >>> test_simple_ssosearch()
    """
    pdf = ssosearch()

    assert not pdf.empty
    assert "1996 TC28" in pdf["f:sso_name"].to_numpy()


def test_ephem() -> None:
    """
    Examples
    --------
    >>> test_ephem()
    """
    names = [
        "1996 TC28",
        "J96T28C",
    ]
    for name in names:
        pdf = ssosearch(n_or_d=name, withEphem=True)

        assert not pdf.empty

        assert "Phase" in pdf.columns

        assert "SDSS:g" in pdf.columns


def test_comet() -> None:
    """
    Examples
    --------
    >>> test_comet()
    """
    # FIXME: change name when Rubin will identify comets
    pdf = ssosearch(n_or_d="10P", withEphem=False)

    assert pdf.empty


def test_temp_designation() -> None:
    """
    Examples
    --------
    >>> test_temp_designation()
    """
    pdf_ephem = ssosearch(n_or_d="1984 CP", withEphem=True)

    assert not pdf_ephem.empty

    assert "Phase" in pdf_ephem.columns


# def test_multiple_sso_names() -> None:
#     """
#     Examples
#     --------
#     >>> test_multiple_sso_names()
#     """
#     # we do not want Rubin & Rubincam
#     pdf = ssosearch(n_or_d="Rubin")
#
#     assert len(pdf["sso_name"].unique()) == 1, pdf["sso_name"].unique()
#     assert len(pdf["i:ssnamenr"].unique()) == 1, pdf["i:ssnamenr"].unique()


def test_bad_request() -> None:
    """
    Examples
    --------
    >>> test_bad_request()
    """
    pdf = ssosearch(n_or_d="kdflsjffld", expected_status=400)

    assert pdf.empty


def test_multiple_ssosearch() -> None:
    """
    Examples
    --------
    >>> test_multiple_ssosearch()
    """
    pdf = ssosearch(n_or_d="1984 CP,J96T28C", withEphem=True)

    assert not pdf.empty

    assert len(pdf.groupby("f:sso_name").count()) == 2, np.unique(pdf["f:sso_name"])

    pdf1 = ssosearch(n_or_d="1984 CP", withEphem=True)
    pdf2 = ssosearch(n_or_d="J96T28C", withEphem=True)

    assert len(pdf) == len(pdf1) + len(pdf2)


if __name__ == "__main__":
    """ Execute the test suite """
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
