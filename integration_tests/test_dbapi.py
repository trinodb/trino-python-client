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

import fixtures
from fixtures import run_presto
import prestodb
import pytest


@pytest.fixture
def presto_connection(run_presto):
    _, host, port = run_presto

    yield prestodb.dbapi.Connection(
        host=host,
        port=port,
        user='test',
        source='test',
        max_attempts=1,
    )


def test_select_query(presto_connection):
    cur = presto_connection.cursor()
    cur.execute('select * from system.runtime.nodes')
    rows = cur.fetchall()
    assert len(rows) > 0
    row = rows[0]
    assert row[0] == 'test'
    assert row[2] == fixtures.PRESTO_VERSION
    columns = dict(cur.description)
    assert columns['node_id'] == 'varchar'
    assert columns['http_uri'] == 'varchar'
    assert columns['node_version'] == 'varchar'
    assert columns['coordinator'] == 'boolean'
    assert columns['state'] == 'varchar'


def test_select_query_result_iteration(presto_connection):
    cur = presto_connection.cursor()
    cur.execute('select custkey from tcph.sf1.customer LIMIT 10')
    rows0 = cur.genall()
    cur.execute('select custkey from tcph.sf1.customer LIMIT 10')
    rows1 = cur.fetchall()

    assert len(list(rows0)) == len(rows1)


def test_select_query_no_result(presto_connection):
    cur = presto_connection.cursor()
    cur.execute('select * from system.runtime.nodes where false')
    rows = cur.fetchall()
    assert len(rows) == 0


def test_select_failed_query(presto_connection):
    cur = presto_connection.cursor()
    with pytest.raises(prestodb.exceptions.PrestoUserError):
        cur.execute('select * from catalog.schema.do_not_exist')
        cur.fetchall()


def test_select_tcph_1000(presto_connection):
    cur = presto_connection.cursor()
    cur.execute('SELECT * FROM tcph.sf1.customer LIMIT 1000')
    rows = cur.fetchall()
    assert len(rows) == 1000
