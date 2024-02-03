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
import subprocess
import time
from contextlib import closing
from uuid import uuid4

import pytest

import trino.logging
from trino.client import ClientSession, TrinoQuery, TrinoRequest
from trino.constants import DEFAULT_PORT

logger = trino.logging.get_logger(__name__)


TRINO_VERSION = os.environ.get("TRINO_VERSION") or "latest"
TRINO_HOST = "127.0.0.1"
TRINO_PORT = 8080


def is_trino_available():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(2)
        result = sock.connect_ex((TRINO_HOST, DEFAULT_PORT))
        if result == 0:
            return True


def get_local_port():
    with closing(socket.socket()) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def get_default_trino_image_tag():
    return "trinodb/trino:" + TRINO_VERSION


def start_trino(image_tag=None):
    if not image_tag:
        image_tag = get_default_trino_image_tag()

    container_id = "trino-python-client-tests-" + uuid4().hex[:7]
    local_port = get_local_port()
    logger.info("starting Docker container")
    docker_run = [
        "docker",
        "run",
        "--rm",
        "-p",
        "{host_port}:{cont_port}".format(host_port=local_port, cont_port=TRINO_PORT),
        "--name",
        container_id,
        image_tag,
    ]
    run = subprocess.Popen(docker_run, universal_newlines=True, stderr=subprocess.PIPE)
    return (container_id, run, "localhost", local_port)


def wait_for_trino_workers(host, port, timeout=180):
    request = TrinoRequest(
        host=host,
        port=port,
        client_session=ClientSession(
            user="test_fixture"
        )
    )
    sql = "SELECT state FROM system.runtime.nodes"
    t0 = time.time()
    while True:
        query = TrinoQuery(request, sql)
        rows = list(query.execute())
        if any(row[0] == "active" for row in rows):
            break
        if time.time() - t0 > timeout:
            raise TimeoutError
        time.sleep(1)


def wait_for_trino_coordinator(stream, timeout=180):
    started_tag = "======== SERVER STARTED ========"
    t0 = time.time()
    for line in iter(stream.readline, b""):
        if line:
            print(line)
        if started_tag in line:
            time.sleep(5)
            return True
        if time.time() - t0 > timeout:
            logger.error("coordinator took longer than %s to start", timeout)
            raise TimeoutError
    return False


def start_local_trino_server(image_tag):
    container_id, proc, host, port = start_trino(image_tag)
    print("trino.server.state starting")
    trino_ready = wait_for_trino_coordinator(proc.stderr)
    if not trino_ready:
        raise Exception("Trino server did not start")
    wait_for_trino_workers(host, port)
    print("trino.server.state ready")
    return container_id, proc, host, port


def start_trino_and_wait(image_tag=None):
    container_id = None
    proc = None
    host = os.environ.get("TRINO_RUNNING_HOST", None)
    if host:
        port = os.environ.get("TRINO_RUNNING_PORT", DEFAULT_PORT)
    else:
        container_id, proc, host, port = start_local_trino_server(
            image_tag
        )

    print("trino.server.hostname {}".format(host))
    print("trino.server.port {}".format(port))
    if proc:
        print("trino.server.pid {}".format(proc.pid))
    if container_id:
        print("trino.server.contained_id {}".format(container_id))
    return container_id, proc, host, port


def stop_trino(container_id, proc):
    subprocess.check_call(["docker", "kill", container_id])


@pytest.fixture(scope="module")
def run_trino():
    if is_trino_available():
        yield None, TRINO_HOST, DEFAULT_PORT
        return

    image_tag = os.environ.get("TRINO_IMAGE")
    if not image_tag:
        image_tag = get_default_trino_image_tag()

    container_id, proc, host, port = start_trino_and_wait(image_tag)
    yield proc, host, port
    if container_id or proc:
        stop_trino(container_id, proc)


def trino_version():
    return TRINO_VERSION
