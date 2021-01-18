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
from datetime import datetime

import pytest
import pytz
from sqlalchemy.engine import create_engine

from conftest import TRINO_VERSION
from trino.exceptions import TrinoUserError, TrinoQueryError


@pytest.fixture
def engine(run_trino):
    _, host, port = run_trino
    engine = create_engine(f'trino://pytest@{host}:{port}/system/runtime')
    yield engine


@pytest.fixture
def connection(engine):
    connection = engine.connect()
    yield connection


def test_engine_and_connection_output_match(engine, connection):
    sql = 'SELECT * FROM system.runtime.nodes'
    engine_result = engine.execute(sql)
    engine_rows = engine_result.fetchall()
    connection_result = connection.execute(sql)
    connection_rows = connection_result.fetchall()
    assert engine_rows == connection_rows


def test_select_query(connection):
    result = connection.execute('SELECT * FROM system.runtime.nodes')
    rows = result.fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row[1] == 'http://172.17.0.2:8080'
    assert row[2] == TRINO_VERSION
    assert row[3] is True
    assert row[4] == 'active'


def test_select_query_no_result(connection):
    result = connection.execute('SELECT * FROM system.runtime.nodes WHERE false')
    rows = result.fetchall()
    assert len(rows) == 0


def test_query_param_types(connection):
    params = (
        None,
        "a'",
        datetime(2020, 1, 1, 0, 0, 0),
        datetime(2020, 1, 1, 0, 0, 0, tzinfo=pytz.utc),
        [1, 2, 3],
        [[1, 2, 3], [4, 5, 6]],
        {'foo': 'bar'},
        True,
        3.33,
        3,
        9223372036854775807,
    )

    n = len(params) - 1

    # Test Python types and values
    result = connection.execute('SELECT ?' + ', ?' * n, params=params)
    rows = result.fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row[0] is None, 'None type parameter value is wrong'
    assert row[1] == "a'", 'String type parameter value is wrong'
    assert row[2] == '2020-01-01 00:00:00.000', 'Timestamp type parameter value is wrong'
    assert row[3] == '2020-01-01 00:00:00.000 UTC', 'Timestamp with time zone type parameter value is wrong'
    assert row[4] == [1, 2, 3], 'Array type parameter value is wrong'
    assert row[5] == [[1, 2, 3], [4, 5, 6]], 'Nested array type parameter value is wrong'
    assert row[6] == {'foo': 'bar'}, 'Map type parameter value is wrong'
    assert row[7] is True, 'Boolean type parameter value is wrong'
    assert row[8] == 3.33, 'Double type parameter value is wrong'
    assert row[9] == 3, 'Integer type parameter value is wrong'
    assert row[10] == 9223372036854775807, 'Big integer type parameter value is wrong'

    # Test Trino types
    result = connection.execute('SELECT TYPEOF(?)' + ', TYPEOF(?)' * n, params=params)
    rows = result.fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == 'unknown', 'None type parameter type is wrong'
    assert row[1] == 'varchar(2)', 'String type parameter type is wrong'
    assert row[2] == 'timestamp(6)', 'Timestamp type parameter type is wrong'
    assert row[3] == 'timestamp(6) with time zone', 'Timestamp with time zone type parameter type is wrong'
    assert row[4] == 'array(integer)', 'Array type parameter type is wrong'
    assert row[5] == 'array(array(integer))', 'Nested array type parameter type is wrong'
    assert row[6] == 'map(varchar(3), varchar(3))', 'Map type parameter type is wrong'
    assert row[7] == 'boolean', 'Boolean type parameter type is wrong'
    assert row[8] == 'double', 'Double type parameter type is wrong'
    assert row[9] == 'integer', 'Integer type parameter type is wrong'
    assert row[10] == 'bigint', 'Big integer type parameter type is wrong'


@pytest.mark.skip(reason='Nan currently not returning the correct python type for nan')
def test_float_nan_query_param(connection):
    result = connection.execute('SELECT ?', params=(float('nan'),))
    rows = result.fetchall()
    assert isinstance(rows[0][0], float)
    assert rows[0][0] == float('nan')


@pytest.mark.skip(reason='Nan currently not returning the correct python type fon inf')
def test_float_inf_query_param(connection):
    result = connection.execute('SELECT ?', params=(float('inf'),))
    rows = result.fetchall()
    assert rows[0][0] == float('inf')

    result = connection.execute('SELECT ?', params=(-float('-inf'),))
    rows = result.fetchall()
    assert rows[0][0] == float('-inf')


@pytest.mark.parametrize('params', [
    'NOT A LIST OR TUPPLE',
    {'invalid', 'params'},
    object,
])
def test_select_query_invalid_params(connection, params):
    with pytest.raises(AssertionError):
        connection.execute('SELECT ?', params=params)


def test_select_failed_query(connection):
    with pytest.raises(TrinoUserError):
        result = connection.execute('SELECT * FROM catalog.schema.do_not_exist')
        result.fetchall()


def test_invalid_query_throws_correct_error(connection):
    with pytest.raises(TrinoQueryError):
        connection.execute('SELECT * FRMO foo')
