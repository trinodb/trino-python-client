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

from contextlib import closing
import logging
import os
import requests
import socket
import subprocess
import time
from uuid import uuid4
import xml.etree.ElementTree as ET

import click
import pytest

from prestodb.client import PrestoQuery, PrestoRequest


logger = logging.getLogger(__name__)


def get_latest_release():
    resp = requests.get(
        'https://repo1.maven.org/maven2/com/'
        'facebook/presto/presto-server/maven-metadata.xml'
    )
    xml_root = ET.fromstring(resp.content)
    try:
        return xml_root.find('versioning').find('release').text.strip()
    except AttributeError:
        raise Exception('not release found')


PRESTO_VERSION = os.environ.get('PRESTO_VERSION', get_latest_release())
PRESTO_HOST = '127.0.0.1'
PRESTO_PORT = 8080


def is_process_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def get_local_port():
    with closing(socket.socket()) as s:
        s.bind(('localhost', 0))
        return s.getsockname()[1]


def start_presto(with_cache=True):
    logger.info('building Docker image')
    image_name = 'presto-server'
    tag = PRESTO_VERSION
    image_tag = image_name + ':' + tag
    docker_build = [
        'docker',
        'build',
        '--build-arg', 'PRESTO_VERSION=' + PRESTO_VERSION,
        '--tag', image_tag,
        '.',
    ]
    if not with_cache:
        docker_build.append('--no-cache')
    subprocess.check_call(docker_build)

    container_id = 'presto-python-client-tests-' + uuid4().hex[:7]
    local_port = get_local_port()
    logger.info('starting Docker container')
    docker_run = [
        'docker',
        'run',
        '--rm',
        '-p', '{host_port}:{cont_port}'.format(
            host_port=local_port,
            cont_port=PRESTO_PORT,
        ),
        '--name', container_id,
        image_tag,
    ]
    run = subprocess.Popen(
        docker_run,
        universal_newlines=True,
        stderr=subprocess.PIPE,
    )
    return (container_id, run, 'localhost', local_port)


def wait_for_presto_workers(host, port, timeout=30):
    request = PrestoRequest(host=host, port=port, user='test_fixture')
    sql = 'SELECT state FROM system.runtime.nodes'
    t0 = time.time()
    while True:
        query = PrestoQuery(request, sql)
        rows = list(query.execute())
        if any(row[0] == 'active' for row in rows):
            break
        if time.time() - t0 > timeout:
            raise TimeoutError
        time.sleep(1)


def wait_for_presto_coordinator(stream):
    started_tag = '======== SERVER STARTED ========'
    for line in iter(stream.readline, b''):
        if started_tag in line:
            time.sleep(5)
            return True
    return False


def start_presto_and_wait(with_cache=True):
    contained_id, proc, host, local_port = start_presto(with_cache=with_cache)
    print('presto.server.state starting')
    presto_ready = wait_for_presto_coordinator(proc.stderr)
    if not presto_ready:
        raise Exception('Presto server did not start')
    wait_for_presto_workers(host, local_port)
    print('presto.server.state ready')
    print('presto.server.hostname {}'.format(host))
    print('presto.server.port {}'.format(local_port))
    print('presto.server.pid {}'.format(proc.pid))
    print('presto.server.contained_id {}'.format(contained_id))
    return contained_id, proc, host, local_port


def stop_presto(container_id, proc):
    subprocess.check_call(['docker', 'kill', container_id])


@pytest.fixture(scope='module')
def run_presto():
    container_id, proc, host, local_port = start_presto_and_wait()
    yield proc, host, local_port
    stop_presto(container_id, proc)


@click.group()
def cli():
    pass


@click.option(
    '--cache/--no-cache',
    default=True,
    help='enable/disable Docker build cache',
)
@click.command()
def presto_server(cache):
    container_id, _, _, _ = start_presto_and_wait(with_cache=cache)


@click.argument('container_id')
@click.command()
def presto_cli(container_id):
    subprocess.call([
        'docker', 'exec', '-t', '-i',
        container_id,
        'bin/presto-cli', '--server', 'localhost:8080',
    ])


@cli.command('list')
def list_():
    subprocess.check_call([
        'docker', 'ps',
        '--filter', 'name=presto-python-client-tests-',
    ])


@cli.command()
def clean():
    cmd = (
        'docker ps '
        '--filter name=presto-python-client-tests- '
        '--format={{.Names}} | '
            'xargs -n 1 docker kill'  # NOQA deliberate additional indent
    )
    subprocess.check_output(cmd, shell=True)


@cli.command()
def tests():
    subprocess.check_call(['./tests_unit'])
    subprocess.check_call(['./tests_integration'])

cli.add_command(presto_server)
cli.add_command(presto_cli)


if __name__ == '__main__':
    cli()
