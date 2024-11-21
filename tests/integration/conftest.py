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
import socket
import sys
from contextlib import closing

import pytest

import trino.logging
from tests.development_server import start_development_server
from tests.development_server import TRINO_HOST
from tests.development_server import TRINO_VERSION
from trino.constants import DEFAULT_PORT

logger = trino.logging.get_logger(__name__)


def is_trino_available(host, port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        if result == 0:
            return True
    return False


def get_local_port():
    with closing(socket.socket()) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def run_trino():
    host = os.environ.get("TRINO_RUNNING_HOST", TRINO_HOST)
    port = os.environ.get("TRINO_RUNNING_PORT", DEFAULT_PORT)

    # Is there any local Trino available
    if is_trino_available(host, port):
        yield host, port
        return

    # Start Trino and MinIO server
    print(f"Could not connect to Trino at {host}:{port}, starting server...")
    local_port = get_local_port()
    with start_development_server(port=local_port):
        yield TRINO_HOST, local_port


def trino_version() -> int:
    if TRINO_VERSION == "latest":
        return sys.maxsize
    return int(TRINO_VERSION)
