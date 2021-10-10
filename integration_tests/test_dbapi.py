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
import math
from datetime import datetime

import pytest
import pytz

import trino
from conftest import TRINO_VERSION
from trino.exceptions import TrinoQueryError
from trino.transaction import IsolationLevel


@pytest.fixture
def trino_connection(run_trino):
    _, host, port = run_trino

    yield trino.dbapi.Connection(
        host=host, port=port, user="test", source="test", max_attempts=1
    )


@pytest.fixture
def trino_connection_with_transaction(run_trino):
    _, host, port = run_trino

    yield trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        max_attempts=1,
        isolation_level=IsolationLevel.READ_UNCOMMITTED,
    )


def test_select_query(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM system.runtime.nodes")
    rows = cur.fetchall()
    assert len(rows) > 0
    row = rows[0]
    assert row[2] == TRINO_VERSION
    columns = dict([desc[:2] for desc in cur.description])
    assert columns["node_id"] == "varchar"
    assert columns["http_uri"] == "varchar"
    assert columns["node_version"] == "varchar"
    assert columns["coordinator"] == "boolean"
    assert columns["state"] == "varchar"


def test_select_query_result_iteration(trino_connection):
    cur0 = trino_connection.cursor()
    cur0.execute("SELECT custkey FROM tpch.sf1.customer LIMIT 10")
    rows0 = cur0.genall()

    cur1 = trino_connection.cursor()
    cur1.execute("SELECT custkey FROM tpch.sf1.customer LIMIT 10")
    rows1 = cur1.fetchall()

    assert len(list(rows0)) == len(rows1)


def test_select_query_result_iteration_statement_params(trino_connection):
    cur = trino_connection.cursor()
    cur.execute(
        """
        SELECT * FROM (
            values
            (1, 'one', 'a'),
            (2, 'two', 'b'),
            (3, 'three', 'c'),
            (4, 'four', 'd'),
            (5, 'five', 'e')
        ) x (id, name, letter)
        WHERE id >= ?
        """,
        params=(3,)  # expecting all the rows with id >= 3
    )


def test_none_query_param(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT ?", params=(None,))
    rows = cur.fetchall()

    assert rows[0][0] is None


def test_string_query_param(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT ?", params=("six'",))
    rows = cur.fetchall()

    assert rows[0][0] == "six'"


@pytest.mark.parametrize(
    'parametric_datetime, datetime, expected_time, description',
    (
        (False, datetime(2020, 1, 1, 0, 0, 0, 0), "2020-01-01 00:00:00.000", "timestamp with time zone"),
        (None, datetime(2020, 1, 1, 0, 0, 0, 0), "2020-01-01 00:00:00.000", "timestamp with time zone"),
        (True, datetime(2020, 1, 1, 0, 0, 0, 100001), "2020-01-01 00:00:00.100001", "timestamp(6) with time zone"),
    )
)
def test_datetime_query_param(run_trino, parametric_datetime, datetime, expected_time, description):
    _, host, port = run_trino

    connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        parametric_datetime=parametric_datetime
    )

    cur = connection.cursor()

    cur.execute("SELECT ?", params=(datetime,))
    rows = cur.fetchall()

    assert rows[0][0] == expected_time

    cur.execute("SELECT ?", params=(datetime.replace(tzinfo=pytz.utc),))
    rows = cur.fetchall()

    assert rows[0][0] == f"{expected_time} UTC"
    assert cur.description[0][1] == description


@pytest.mark.parametrize(
    'parametric_datetime, time, description',
    (
        (False, "00:00:00.000", "timestamp with time zone"),
        (None, "00:00:00.000", "timestamp with time zone"),
        (True, "00:00:00.100001", "time(6) with time zone"),
    )
)
def test_time_query_param(run_trino, parametric_datetime, time, description):
    _, host, port = run_trino

    connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        parametric_datetime=parametric_datetime
    )

    cur = connection.cursor()

    cur.execute(f"SELECT TIME '{time}'")
    rows = cur.fetchall()

    assert rows[0][0] == time

    cur.execute(f"SELECT TIME '{time} +00:00'")
    rows = cur.fetchall()

    assert rows[0][0] == f"{time}+00:00"
    assert cur.description[0][1] == description


@pytest.mark.parametrize(
    'parametric_datetime, timestamp, description',
    (
        (False, "2020-01-01 00:00:00.000", "timestamp with time zone"),
        (None, "2020-01-01 00:00:00.000", "timestamp with time zone"),
        (True, "2020-01-01 00:00:00.100001", "timestamp(6) with time zone"),
    )
)
def test_timestamp_query_param(run_trino, parametric_datetime, timestamp, description):
    _, host, port = run_trino

    connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        parametric_datetime=parametric_datetime
    )

    cur = connection.cursor()

    cur.execute(f"SELECT TIMESTAMP '{timestamp}'")
    rows = cur.fetchall()

    assert rows[0][0] == timestamp

    cur.execute(f"SELECT TIMESTAMP '{timestamp} +00:00'")
    rows = cur.fetchall()

    assert rows[0][0] == f"{timestamp} UTC"
    assert cur.description[0][1] == description


@pytest.mark.parametrize(
    'parametric_datetime, timestamp, description',
    (
        (False, "2020-01-01 00:00:00.000", "timestamp with time zone"),
        (None, "2020-01-01 00:00:00.000", "timestamp with time zone"),
        (True, "2020-01-01 00:00:00.100001", "timestamp(6) with time zone"),
    )
)
def test_interval_query_param(run_trino, parametric_datetime, timestamp, description):
    _, host, port = run_trino

    connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        parametric_datetime=parametric_datetime
    )

    cur = connection.cursor()

    cur.execute(f"SELECT DATE_ADD('millisecond', 0, TIMESTAMP '{timestamp}')")
    rows = cur.fetchall()

    assert rows[0][0] == timestamp

    cur.execute(f"SELECT DATE_ADD('millisecond', 0, TIMESTAMP '{timestamp} +00:00')")
    rows = cur.fetchall()

    assert rows[0][0] == f"{timestamp} UTC"
    assert cur.description[0][1] == description


def test_array_query_param(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT ?", params=([1, 2, 3],))
    rows = cur.fetchall()

    assert rows[0][0] == [1, 2, 3]

    cur.execute("SELECT ?", params=([[1, 2, 3], [4, 5, 6]],))
    rows = cur.fetchall()

    assert rows[0][0] == [[1, 2, 3], [4, 5, 6]]

    cur.execute("SELECT TYPEOF(?)", params=([1, 2, 3],))
    rows = cur.fetchall()

    assert rows[0][0] == "array(integer)"


def test_dict_query_param(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT ?", params=({"foo": "bar"},))
    rows = cur.fetchall()

    assert rows[0][0] == {"foo": "bar"}

    cur.execute("SELECT TYPEOF(?)", params=({"foo": "bar"},))
    rows = cur.fetchall()

    assert rows[0][0] == "map(varchar(3), varchar(3))"


def test_boolean_query_param(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT ?", params=(True,))
    rows = cur.fetchall()

    assert rows[0][0] is True

    cur.execute("SELECT ?", params=(False,))
    rows = cur.fetchall()

    assert rows[0][0] is False


def test_float_query_param(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT ?", params=(1.1,))
    rows = cur.fetchall()

    assert cur.description[0][1] == "double"
    assert rows[0][0] == 1.1


@pytest.mark.skip(reason="Nan currently not returning the correct python type for nan")
def test_float_nan_query_param(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT ?", params=(float("nan"),))
    rows = cur.fetchall()

    assert cur.description[0][1] == "double"
    assert isinstance(rows[0][0], float)
    assert math.isnan(rows[0][0])


@pytest.mark.skip(reason="Nan currently not returning the correct python type fon inf")
def test_float_inf_query_param(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT ?", params=(float("inf"),))
    rows = cur.fetchall()

    assert rows[0][0] == float("inf")

    cur.execute("SELECT ?", params=(-float("-inf"),))
    rows = cur.fetchall()

    assert rows[0][0] == float("-inf")


def test_int_query_param(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT ?", params=(3,))
    rows = cur.fetchall()

    assert rows[0][0] == 3
    assert cur.description[0][1] == "integer"

    cur.execute("SELECT ?", params=(9223372036854775807,))
    rows = cur.fetchall()

    assert rows[0][0] == 9223372036854775807
    assert cur.description[0][1] == "bigint"


@pytest.mark.parametrize('params', [
    'NOT A LIST OR TUPPLE',
    {'invalid', 'params'},
    object,
])
def test_select_query_invalid_params(trino_connection, params):
    cur = trino_connection.cursor()
    with pytest.raises(AssertionError):
        cur.execute('SELECT ?', params=params)


def test_select_cursor_iteration(trino_connection):
    cur0 = trino_connection.cursor()
    cur0.execute("SELECT nationkey FROM tpch.sf1.nation")
    rows0 = []
    for row in cur0:
        rows0.append(row)

    cur1 = trino_connection.cursor()
    cur1.execute("SELECT nationkey FROM tpch.sf1.nation")
    rows1 = cur1.fetchall()

    assert len(rows0) == len(rows1)
    assert sorted(rows0) == sorted(rows1)


def test_select_query_no_result(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM system.runtime.nodes WHERE false")
    rows = cur.fetchall()
    assert len(rows) == 0


def test_select_query_stats(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")

    query_id = cur.stats["queryId"]
    completed_splits = cur.stats["completedSplits"]
    cpu_time_millis = cur.stats["cpuTimeMillis"]
    processed_bytes = cur.stats["processedBytes"]
    processed_rows = cur.stats["processedRows"]
    wall_time_millis = cur.stats["wallTimeMillis"]

    while cur.fetchone() is not None:
        assert query_id == cur.stats["queryId"]
        assert completed_splits <= cur.stats["completedSplits"]
        assert cpu_time_millis <= cur.stats["cpuTimeMillis"]
        assert processed_bytes <= cur.stats["processedBytes"]
        assert processed_rows <= cur.stats["processedRows"]
        assert wall_time_millis <= cur.stats["wallTimeMillis"]

        query_id = cur.stats["queryId"]
        completed_splits = cur.stats["completedSplits"]
        cpu_time_millis = cur.stats["cpuTimeMillis"]
        processed_bytes = cur.stats["processedBytes"]
        processed_rows = cur.stats["processedRows"]
        wall_time_millis = cur.stats["wallTimeMillis"]


def test_select_failed_query(trino_connection):
    cur = trino_connection.cursor()
    with pytest.raises(trino.exceptions.TrinoUserError):
        cur.execute("SELECT * FROM catalog.schema.do_not_exist")
        cur.fetchall()


def test_select_tpch_1000(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
    rows = cur.fetchall()
    assert len(rows) == 1000


def test_cancel_query(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM tpch.sf1.customer")
    cur.fetchone()  # TODO (https://github.com/trinodb/trino/issues/2683) test with and without .fetchone
    cur.cancel()  # would raise an exception if cancel fails

    cur = trino_connection.cursor()
    with pytest.raises(Exception) as cancel_error:
        cur.cancel()
    assert "Cancel query failed; no running query" in str(cancel_error.value)


def test_session_properties(run_trino):
    _, host, port = run_trino

    connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        session_properties={"query_max_run_time": "10m", "query_priority": "1"},
        max_attempts=1,
    )
    cur = connection.cursor()
    cur.execute("SHOW SESSION")
    rows = cur.fetchall()
    assert len(rows) > 2
    for prop, value, _, _, _ in rows:
        if prop == "query_max_run_time":
            assert value == "10m"
        elif prop == "query_priority":
            assert value == "1"


def test_transaction_single(trino_connection_with_transaction):
    connection = trino_connection_with_transaction
    for _ in range(3):
        cur = connection.cursor()
        cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows = cur.fetchall()
        connection.commit()
        assert len(rows) == 1000


def test_transaction_rollback(trino_connection_with_transaction):
    connection = trino_connection_with_transaction
    for _ in range(3):
        cur = connection.cursor()
        cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows = cur.fetchall()
        connection.rollback()
        assert len(rows) == 1000


def test_transaction_multiple(trino_connection_with_transaction):
    with trino_connection_with_transaction as connection:
        cur1 = connection.cursor()
        cur1.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows1 = cur1.fetchall()

        cur2 = connection.cursor()
        cur2.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows2 = cur2.fetchall()

    assert len(rows1) == 1000
    assert len(rows2) == 1000


def test_invalid_query_throws_correct_error(trino_connection):
    """Tests that an invalid query raises the correct exception
    """
    cur = trino_connection.cursor()
    with pytest.raises(TrinoQueryError):
        cur.execute(
            """
            SELECT * FRMO foo WHERE x = ?;
            """,
            params=(3,),
        )


def test_eager_loading_cursor_description(trino_connection):
    description_expected = [
        ('node_id', 'varchar', None, None, None, None, None),
        ('http_uri', 'varchar', None, None, None, None, None),
        ('node_version', 'varchar', None, None, None, None, None),
        ('coordinator', 'boolean', None, None, None, None, None),
        ('state', 'varchar', None, None, None, None, None),
    ]
    cur = trino_connection.cursor()
    cur.execute('SELECT * FROM system.runtime.nodes')
    description_before = cur.description

    assert description_before is not None
    assert len(description_before) == len(description_expected)
    assert all([b == e] for b, e in zip(description_before, description_expected))

    cur.fetchone()
    description_after = cur.description
    assert description_after is not None
    assert len(description_after) == len(description_expected)
    assert all([a == e] for a, e in zip(description_after, description_expected))
