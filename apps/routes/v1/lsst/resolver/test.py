# Copyright 2026 AstroLab Software
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
import io
import sys

import pandas as pd
import requests

APIURL = sys.argv[1]


def resolver(
    resolver="simbad",
    name_or_id="Markarian 2",
    nmax=None,
    reverse=None,
    output_format="json",
):
    """Resolve a name using the Fink REST API"""
    payload: dict = {
        "resolver": resolver,
        "name_or_id": name_or_id,
        "output-format": output_format,
    }

    if reverse is not None:
        payload.update({"reverse": True})

    if nmax is not None:
        payload.update({"nmax": nmax})

    r = requests.post(f"{APIURL}/api/v1/resolver", json=payload)

    assert r.status_code == 200, r.content

    if output_format == "json":
        pdf = pd.read_json(io.BytesIO(r.content))
    else:
        pdf = pd.read_csv(io.BytesIO(r.content))

    return pdf


def test_tns_fulltable() -> None:
    """
    Examples
    --------
    >>> test_tns_fulltable()
    """
    pdf = resolver(resolver="tns", name_or_id="", nmax=100000)

    assert not pdf.empty

    assert len(pdf) > 10000, len(pdf)


def test_tns_resolver() -> None:
    """
    Examples
    --------
    >>> test_tns_resolver()
    """
    pdf = resolver(resolver="tns", name_or_id="SN 2024abtt")

    assert not pdf.empty

    assert pdf["f:fullname"].unique().tolist() == ["SN 2024abtt"], pdf

    cols = [
        "f:declination",
        "f:fullname",
        "f:internalname",
        "f:ra",
        "f:type",
        "f:redshift",
    ]
    for col in cols:
        assert col in pdf.columns, [col, pdf.columns]


def test_tns_lower_case() -> None:
    """
    Examples
    --------
    >>> test_tns_lower_case()
    """
    pdf = resolver(resolver="tns", name_or_id="sn 2024abtt")

    assert not pdf.empty

    assert pdf["f:fullname"].unique().tolist() == ["SN 2024abtt"], pdf


def test_reverse_tns_resolver() -> None:
    """
    Examples
    --------
    >>> test_reverse_tns_resolver()
    """
    pdf = resolver(resolver="tns", name_or_id="313853497671417905", reverse=True)

    assert len(pdf) == 1, pdf

    assert pdf["r:diaObjectId"].to_numpy()[0] == 313853497671417905, pdf


def test_nmax() -> None:
    """
    Examples
    --------
    >>> test_nmax()
    """
    pdf = resolver(resolver="tns", name_or_id="SN 2024abtt", nmax=2)

    assert len(pdf) == 2, len(pdf)


def test_simbad_resolver() -> None:
    """
    Examples
    --------
    >>> test_simbad_resolver()
    """
    pdf = resolver()

    assert len(pdf) == 1, len(pdf)

    cols = [
        "name",
        "oid",
        "oname",
        "otype",
        "jpos",
        "jradeg",
        "jdedeg",
        "refPos",
        "MType",
        "nrefs",
    ]
    for col in cols:
        assert col in pdf.columns, [col, pdf.columns]


def test_simbad_resolver_csv() -> None:
    """
    Examples
    --------
    >>> test_simbad_resolver_csv()
    """
    pdf = resolver(output_format="csv")

    assert len(pdf) == 1, len(pdf)


def test_reverse_simbad_resolver() -> None:
    """
    Examples
    --------
    >>> test_reverse_simbad_resolver()
    """
    pdf = resolver(name_or_id="314003014107006318", reverse=True)

    assert len(pdf) == 1, pdf

    assert "f:main_label_crossmatch" in pdf.columns, pdf.columns
    assert pdf["r:diaObjectId"].to_numpy()[0] == 314003014107006318, pdf


def test_ssodnet_resolver() -> None:
    """
    Examples
    --------
    >>> test_ssodnet_resolver()
    """
    pdf = resolver(resolver="ssodnet", name_or_id="1996 TC28")

    assert not pdf.empty

    assert pdf["r:ssObjectId"].unique().tolist() == [20892053780183107], pdf
    assert "J96T28C" in pdf["r:packed_primary_provisional_designation"].to_numpy()


def test_reverse_ssodnet_resolver() -> None:
    """
    Examples
    --------
    >>> test_reverse_ssodnet_resolver()
    """
    pdf = resolver(resolver="ssodnet", name_or_id="20892053780183107", reverse=True)

    assert len(pdf) == 1, pdf

    assert (
        pdf["r:unpacked_primary_provisional_designation"].to_numpy()[0] == "1996 TC28"
    ), pdf


def test_bad_request() -> None:
    """
    Examples
    --------
    >>> test_bad_request()
    """
    r = requests.post(f"{APIURL}/api/v1/resolver", json={"resolver": "simbad"})

    assert r.status_code == 400, r.content
    assert b"name_or_id" in r.content, r.content


if __name__ == "__main__":
    """ Execute the test suite """
    import doctest
    import sys

    sys.exit(doctest.testmod()[0])
