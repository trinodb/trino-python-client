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
from prestodb.constants import DEFAULT_PORT


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


PRESTO_VERSION = os.environ.get('PRESTO_VERSION') or get_latest_release()
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


def build_image(image_tag, with_cache=True):
    logger.info('building Docker image')
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


def get_default_presto_image_tag():
    image_name = 'presto-server'
    tag = PRESTO_VERSION
    return image_name + ':' + tag


def start_presto(image_tag=None, build=True, with_cache=True):
    if not image_tag:
        image_tag = get_default_presto_image_tag()
    if build:
        build_image(image_tag, with_cache)

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


def start_local_presto_server(image_tag, build=True, with_cache=True):
    container_id, proc, host, port = start_presto(
            image_tag,
            build,
            with_cache,
        )
    print('presto.server.state starting')
    presto_ready = wait_for_presto_coordinator(proc.stderr)
    if not presto_ready:
        raise Exception('Presto server did not start')
    wait_for_presto_workers(host, port)
    print('presto.server.state ready')
    return container_id, proc, host, port


def start_presto_and_wait(image_tag=None, build=True, with_cache=True):
    container_id = None
    proc = None
    host = os.environ.get('PRESTO_RUNNING_HOST', None)
    if host:
        port = os.environ.get('PRESTO_RUNNING_PORT', DEFAULT_PORT)
    else:
        container_id, proc, host, port = start_local_presto_server(
            image_tag,
            build,
            with_cache,
        )

    print('presto.server.hostname {}'.format(host))
    print('presto.server.port {}'.format(port))
    if proc:
        print('presto.server.pid {}'.format(proc.pid))
    if container_id:
        print('presto.server.contained_id {}'.format(container_id))
    return container_id, proc, host, port


def stop_presto(container_id, proc):
    subprocess.check_call(['docker', 'kill', container_id])


def find_images(name):
    assert name
    output = subprocess.check_output([
        'docker', 'images',
        '--format', '{{.Repository}}:{{.Tag}}',
        name,
    ])
    return [line.decode() for line in output.splitlines()]


def image_exists(name):
    images = find_images(name)
    return images and images[0].strip() == name


@pytest.fixture(scope='module')
def run_presto():
    image_tag = os.environ.get('PRESTO_IMAGE')
    if not image_tag:
        image_tag = get_default_presto_image_tag()

    need_build = not image_exists(image_tag)
    container_id, proc, host, port = start_presto_and_wait(
        image_tag,
        need_build,
    )
    yield proc, host, port
    if container_id or proc:
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


@click.argument('container_id', required=False)
@click.command()
def presto_cli(container_id=None):
    if not container_id:
        container_id = os.environ.get('PRESTO_CONTAINER')
        if not container_id:
            raise ValueError('no container specified')
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
