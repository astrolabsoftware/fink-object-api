# Copyright 2022 AstroLab Software
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


def bayestartest(
    bayestar="apps/routes/v1/skymap/bayestar.fits.gz",
    event_name="",
    credible_level=0.1,
    n_day_before=1,
    n_day_after=6,
    output_format="json",
):
    """Perform a GW search in the Science Portal using the Fink REST API"""
    if event_name != "":
        payload = {
            "event_name": event_name,
            "credible_level": credible_level,
            "output-format": output_format,
        }
    else:
        data = open(bayestar, "rb").read()
        payload = {
            "bayestar": str(data),
            "credible_level": credible_level,
            "output-format": output_format,
        }

    r = requests.post("{}/api/v1/skymap".format(APIURL), json=payload)

    assert r.status_code == 200, r.content

    pdf = pd.read_json(io.BytesIO(r.content))

    return pdf


def test_bayestar() -> None:
    """
    Examples
    --------
    >>> test_bayestar()
    """
    pdf = bayestartest()

    assert len(pdf) == 14, len(pdf)

    a = (
        pdf.groupby("d:classification")
        .count()
        .sort_values("i:objectId", ascending=False)["i:objectId"]
        .to_dict()
    )

    assert a["Unknown"] == 4, a


def test_name_bayestar() -> None:
    """
    Examples
    --------
    >>> test_name_bayestar()
    """
    pdf1 = bayestartest(event_name="S200219ac")
    pdf2 = bayestartest()

    assert pdf1.equals(pdf2)


if __name__ == "__main__":
    """ Execute the test suite """
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
