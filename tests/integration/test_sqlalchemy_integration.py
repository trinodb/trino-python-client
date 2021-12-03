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
import pytest
import sqlalchemy as sqla

from trino.transaction import IsolationLevel


@pytest.fixture
def trino_connection(run_trino, catalog):
    _, host, port = run_trino
    engine = sqla.create_engine(f"trino://test@{host}:{port}/{catalog}",
                                connect_args={"source": "test", "max_attempts": 1})
    yield engine.connect()


@pytest.fixture
def trino_connection_with_transaction(run_trino, catalog):
    _, host, port = run_trino
    engine = sqla.create_engine(f"trino://test@{host}:{port}/{catalog}",
                                connect_args={"source": "test", "max_attempts": 1,
                                              "isolation_level": IsolationLevel.READ_UNCOMMITTED})
    yield engine.connect()


@pytest.mark.parametrize('catalog', ['system'])
def test_select_query(trino_connection, trino_version):
    metadata = sqla.MetaData()
    nodes = sqla.Table('nodes', metadata, schema='runtime', autoload_with=trino_connection)
    query = sqla.select(nodes)
    print(query.columns['node_id'])
    # assert columns["node_id"] == "varchar"
    # assert columns["http_uri"] == "varchar"
    # assert columns["node_version"] == "varchar"
    # assert columns["coordinator"] == "boolean"
    # assert columns["state"] == "varchar"
    result = trino_connection.execute(query)
    rows = result.fetchall()
    assert len(rows) > 0
    row = rows[0]
    assert row[2] == trino_version
