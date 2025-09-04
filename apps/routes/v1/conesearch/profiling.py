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
"""Call run_conesearch"""

from apps.routes.v1.ztf.conesearch.utils import run_conesearch

# Conesearch in the
payload_galactic = {"ra": 347.661215, "dec": 61.588129, "radius": 3600}

payload_extragal = {"ra": 193.821739, "dec": 2.897311, "radius": 10.0}

pdf = run_conesearch(payload_extragal)
print("{} objects".format(len(pdf)))
