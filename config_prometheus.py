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
import uuid
import glob
from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics


def when_ready(server):
    path = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
    # cleaning up old .db files at server startup
    if path:
        for f in glob.glob(os.path.join(path, "*.db")):
            os.remove(f)
    PORT = int(os.getenv("PROMETHEUS_METRIC_PORT", 9000))
    GunicornPrometheusMetrics.start_http_server_when_ready(PORT)


class GunicornWorkerIDsPool:
    """manage a collection of worker IDs for a Gunicorn server"""

    def __init__(self):
        """
        Initialize an empty pool of reserved worker IDs.

        :param self: instance of the class
        """
        self._reserved_IDs_pool = []

    def get_id(self):
        """
        Return a worker ID.
        Reuse one from the pool if available, otherwise generate a new UUID.

        :param self: instance of the class
        :return str, a worker ID
        """
        if not self._reserved_IDs_pool:
            return str(uuid.uuid4())
        return self._reserved_IDs_pool.pop()

    def add_id(self, worker_id):
        """
        Add a worker ID back to the pool for future reuse.

        :param self: Instance of the class
        :param worker_id: str, the worker ID to add back to the pool
        """
        self._reserved_IDs_pool.append(worker_id)


# Instantiate once in the Gunicorn master process
gunicorn_worker_ids_pool = GunicornWorkerIDsPool()

# redifine gunicorn hooks for worker ID management and prometheus metrics integration


def child_exit(server, worker):
    """
    called when a worker exits.
    Returns the worker_id to the pool so it can be reused,
    and marks the worker process ad dead for prometheus monitoring .

    :param server: Gunicorn master server instance
    :param worker: Gunicorn worker instance that exited
    """
    worker_id = getattr(worker, "worker_id", None)
    if worker_id:
        gunicorn_worker_ids_pool.add_id(worker_id)
    GunicornPrometheusMetrics.mark_process_dead_on_child_exit(worker.pid)


def pre_fork(server, worker):
    """
    called by gunicorn master before a worker is forked. Assigns
    a unique worker_id from the pool to the worker .

    :param server: Gunicorn master server instance
    :param worker: Gunicorn worker instance about to be forked
    """
    setattr(worker, "worker_id", gunicorn_worker_ids_pool.get_id())


def post_fork(server, worker):
    """
    called by gunicorn master after a worker is forked. Expose
    the worker_id in the env for the worker process .

    :param server: Gunicorn master server instance
    :param worker: Gunicorn worker instance that was forked
    """
    os.environ["GUNICORN_WORKER_ID"] = getattr(worker, "worker_id", None)
