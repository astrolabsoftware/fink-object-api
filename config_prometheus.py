# Copyright 2025 AstroLab Software
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
import os
import glob
from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics


def when_ready(server):
    PORT = int(os.getenv("PROMETHEUS_METRIC_PORT", 9090))
    GunicornPrometheusMetrics.start_http_server_when_ready(PORT)


## overriding the child_exit method so that it does what we want
## instead of using the function GunicornPrometheusMetrics.mark_process_dead

def child_exit(server, worker):
    """Deletes the files associated with the dead worker """

    path = os.environ.get('PROMETHEUS_MULTIPROC_DIR')

    pid = worker.pid

    # Deletion of counter, gauge, and histogram files associated with this worker
    for db_file in glob.glob(os.path.join(path, f'counter_{pid}.db')):
        os.remove(db_file)
        print(f"File deleted : {db_file}")

    for db_file in glob.glob(os.path.join(path, f'gauge_max_{pid}.db')):
        os.remove(db_file)
        print(f"File deleted : {db_file}")

    for db_file in glob.glob(os.path.join(path, f'histogram_{pid}.db')):
        os.remove(db_file)
        print(f"File deleted : {db_file}")
