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
## Script to launch the python test suite and measure the coverage.
## Must be launched as fink_test
set -e
message_help="""
Profile a route\n\n
Usage:\n
    \t./profile_route.sh --route <route_path>\n\n
"""

export ROOTPATH=`pwd`
export PYTHONPATH=$PYTHONPATH:$ROOTPATH

# Grab the command line arguments
NO_SPARK=false
while [ "$#" -gt 0 ]; do
  case "$1" in
    --route)
      ROUTE_PATH=$2
      shift 2
      ;;
    -h)
        echo -e $message_help
        exit
        ;;
  esac
done

kernprof -l $ROUTE_PATH/profiling.py
python -m line_profiler -rmt "profiling.py.lprof"

