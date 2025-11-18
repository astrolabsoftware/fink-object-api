#!/bin/bash
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
BASEURL=https://hrivnac.web.cern.ch/Activities/Packages/Lomikel
CLIENTVERSION=03.08.00

files="Lomikel-$CLIENTVERSION-ext.jar Lomikel-$CLIENTVERSION.exe.jar Lomikel-$CLIENTVERSION-HBase.jar Lomikel-$CLIENTVERSION-HBase.exe.jar Lomikel-$CLIENTVERSION.jar"

for file in $files; do
        echo $file
        wget --directory-prefix=. $BASEURL/$file
done
