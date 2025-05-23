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
"""Call extract_object_data"""

from apps.routes.v1.resolver.utils import resolver_name

payload_tns = {"resolver": "tns", "name": "ZTF24abxxltd"}
payload_sso = {"resolver": "ssodnet", "name": "Julienpeloton"}
payload_simbad = {"resolver": "simbad", "name": "Coma cluster"}

resolver_name(payload_sso)
