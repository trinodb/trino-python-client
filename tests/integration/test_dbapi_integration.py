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
from datetime import datetime, time, date, timezone, timedelta
from decimal import Decimal

import pytest
import pytz
import requests

import trino
from tests.integration.conftest import trino_version
from trino import constants
from trino.exceptions import TrinoQueryError, TrinoUserError, NotSupportedError
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


@pytest.fixture
def trino_connection_in_autocommit(run_trino):
    _, host, port = run_trino

    yield trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        max_attempts=1,
        isolation_level=IsolationLevel.AUTOCOMMIT,
    )


def test_select_query(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM system.runtime.nodes")
    rows = cur.fetchall()
    assert len(rows) > 0
    row = rows[0]
    if trino_version() == "latest":
        assert row[2] is not None
    else:
        assert row[2] == trino_version()
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


def test_execute_many(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("CREATE TABLE memory.default.test_execute_many (key int, value varchar)")
    cur.fetchall()
    operation = "INSERT INTO memory.default.test_execute_many (key, value) VALUES (?, ?)"
    cur.executemany(operation, [(1, "value1")])
    cur.fetchall()
    cur.execute("SELECT * FROM memory.default.test_execute_many ORDER BY key")
    rows = cur.fetchall()
    assert len(list(rows)) == 1
    assert rows[0] == [1, "value1"]

    operation = "INSERT INTO memory.default.test_execute_many (key, value) VALUES (?, ?)"
    cur.executemany(operation, [(2, "value2"), (3, "value3")])
    cur.fetchall()

    cur.execute("SELECT * FROM memory.default.test_execute_many ORDER BY key")
    rows = cur.fetchall()
    assert len(list(rows)) == 3
    assert rows[0] == [1, "value1"]
    assert rows[1] == [2, "value2"]
    assert rows[2] == [3, "value3"]


def test_execute_many_without_params(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("CREATE TABLE memory.default.test_execute_many_without_param (value varchar)")
    cur.fetchall()
    with pytest.raises(TrinoUserError) as e:
        cur.executemany("INSERT INTO memory.default.test_execute_many_without_param (value) VALUES (?)", [])
        cur.fetchall()
    assert "Incorrect number of parameters: expected 1 but found 0" in str(e.value)


def test_execute_many_select(trino_connection):
    cur = trino_connection.cursor()
    with pytest.raises(NotSupportedError) as e:
        cur.executemany("SELECT ?, ?", [(1, "value1"), (2, "value2")])
    assert "Query must return update type" in str(e.value)


@pytest.mark.parametrize("connection_experimental_python_types,cursor_experimental_python_types,expected",
                         [
                             (None, None, False),
                             (None, False, False),
                             (None, True, True),
                             (False, None, False),
                             (False, False, False),
                             (False, True, True),
                             (True, None, True),
                             (True, False, False),
                             (True, True, True),
                         ])
def test_experimental_python_types_with_connection_and_cursor(
        connection_experimental_python_types,
        cursor_experimental_python_types,
        expected,
        run_trino
):
    _, host, port = run_trino

    connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        experimental_python_types=connection_experimental_python_types,
    )

    cur = connection.cursor(experimental_python_types=cursor_experimental_python_types)

    cur.execute("""
    SELECT
        DECIMAL '0.142857',
        DATE '2018-01-01',
        TIMESTAMP '2019-01-01 00:00:00.000+01:00',
        TIMESTAMP '2019-01-01 00:00:00.000 UTC',
        TIMESTAMP '2019-01-01 00:00:00.000',
        TIME '00:00:00.000'
    """)
    rows = cur.fetchall()

    if expected:
        assert rows[0][0] == Decimal('0.142857')
        assert rows[0][1] == date(2018, 1, 1)
        assert rows[0][2] == datetime(2019, 1, 1, tzinfo=timezone(timedelta(hours=1)))
        assert rows[0][3] == datetime(2019, 1, 1, tzinfo=pytz.timezone('UTC'))
        assert rows[0][4] == datetime(2019, 1, 1)
        assert rows[0][5] == time(0, 0, 0, 0)
    else:
        for value in rows[0]:
            assert isinstance(value, str)

        assert rows[0][0] == '0.142857'
        assert rows[0][1] == '2018-01-01'
        assert rows[0][2] == '2019-01-01 00:00:00.000 +01:00'
        assert rows[0][3] == '2019-01-01 00:00:00.000 UTC'
        assert rows[0][4] == '2019-01-01 00:00:00.000'
        assert rows[0][5] == '00:00:00.000'


def test_decimal_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT ?", params=(Decimal('0.142857'),))
    rows = cur.fetchall()

    assert rows[0][0] == Decimal('0.142857')


def test_null_decimal(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT CAST(NULL AS DECIMAL)")
    rows = cur.fetchall()

    assert rows[0][0] is None


def test_biggest_decimal(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = Decimal('99999999999999999999999999999999999999')
    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


def test_smallest_decimal(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = Decimal('-99999999999999999999999999999999999999')
    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


def test_highest_precision_decimal(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = Decimal('0.99999999999999999999999999999999999999')
    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


def test_datetime_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = datetime(2020, 1, 1, 16, 43, 22, 320000)

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert cur.description[0][1] == "timestamp"


def test_datetime_with_utc_time_zone_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=pytz.timezone('UTC'))

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert cur.description[0][1] == "timestamp with time zone"


def test_datetime_with_numeric_offset_time_zone_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    tz = timezone(-timedelta(hours=5, minutes=30))

    params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=tz)

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert cur.description[0][1] == "timestamp with time zone"


def test_datetime_with_named_time_zone_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=pytz.timezone('America/Los_Angeles'))

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert cur.description[0][1] == "timestamp with time zone"


def test_datetime_with_trailing_zeros(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT TIMESTAMP '2001-08-22 03:04:05.321000'")
    rows = cur.fetchall()

    assert rows[0][0] == datetime.strptime("2001-08-22 03:04:05.321000", "%Y-%m-%d %H:%M:%S.%f")


def test_null_datetime_with_time_zone(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT CAST(NULL AS TIMESTAMP WITH TIME ZONE)")
    rows = cur.fetchall()

    assert rows[0][0] is None


def test_datetime_with_time_zone_numeric_offset(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT TIMESTAMP '2001-08-22 03:04:05.321 -08:00'")
    rows = cur.fetchall()

    assert rows[0][0] == datetime.strptime("2001-08-22 03:04:05.321 -08:00", "%Y-%m-%d %H:%M:%S.%f %z")


def test_datetimes_with_time_zone_in_dst_gap_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    # This is a datetime that lies within a DST transition and not actually exists.
    params = datetime(2021, 3, 28, 2, 30, 0, tzinfo=pytz.timezone('Europe/Brussels'))
    with pytest.raises(trino.exceptions.TrinoUserError):
        cur.execute("SELECT ?", params=(params,))
        cur.fetchall()


def test_doubled_datetimes(trino_connection):
    # Trino doesn't distinguish between doubled datetimes that lie within a DST transition. See also
    # See also https://github.com/trinodb/trino/issues/5781
    cur = trino_connection.cursor(experimental_python_types=True)

    params = pytz.timezone('US/Eastern').localize(datetime(2002, 10, 27, 1, 30, 0), is_dst=True)

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == datetime(2002, 10, 27, 1, 30, 0, tzinfo=pytz.timezone('US/Eastern'))

    cur = trino_connection.cursor(experimental_python_types=True)

    params = pytz.timezone('US/Eastern').localize(datetime(2002, 10, 27, 1, 30, 0), is_dst=False)

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == datetime(2002, 10, 27, 1, 30, 0, tzinfo=pytz.timezone('US/Eastern'))


def test_date_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = datetime(2020, 1, 1, 0, 0, 0).date()

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


def test_null_date(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT CAST(NULL AS DATE)")
    rows = cur.fetchall()

    assert rows[0][0] is None


def test_unsupported_python_dates(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    # dates below python min (1-1-1) or above max date (9999-12-31) are not supported
    for unsupported_date in [
        '-0001-01-01',
        '0000-01-01',
        '10000-01-01',
        '-4999999-01-01',  # Trino min date
        '5000000-12-31',  # Trino max date
    ]:
        with pytest.raises(trino.exceptions.TrinoDataError):
            cur.execute(f"SELECT DATE '{unsupported_date}'")
            cur.fetchall()


def test_supported_special_dates_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    for params in (
            # min python date
            date(1, 1, 1),
            # before julian->gregorian switch
            date(1500, 1, 1),
            # During julian->gregorian switch
            date(1752, 9, 4),
            # before epoch
            date(1952, 4, 3),
            date(1970, 1, 1),
            date(1970, 2, 3),
            # summer on northern hemisphere (possible DST)
            date(2017, 7, 1),
            # winter on northern hemisphere (possible DST on southern hemisphere)
            date(2017, 1, 1),
            # winter on southern hemisphere (possible DST on northern hemisphere)
            date(2017, 12, 31),
            date(1983, 4, 1),
            date(1983, 10, 1),
            # max python date
            date(9999, 12, 31),
    ):
        cur.execute("SELECT ?", params=(params,))
        rows = cur.fetchall()

        assert rows[0][0] == params


def test_time_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = time(12, 3, 44, 333000)

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


def test_time_with_named_time_zone_query_param(trino_connection):
    with pytest.raises(trino.exceptions.NotSupportedError):
        cur = trino_connection.cursor()

        params = time(16, 43, 22, 320000, tzinfo=pytz.timezone('Asia/Shanghai'))

        cur.execute("SELECT ?", params=(params,))


def test_time_with_numeric_offset_time_zone_query_param(trino_connection):
    with pytest.raises(trino.exceptions.NotSupportedError):
        cur = trino_connection.cursor()

        tz = timezone(-timedelta(hours=8, minutes=0))

        params = time(16, 43, 22, 320000, tzinfo=tz)

        cur.execute("SELECT ?", params=(params,))


def test_time(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT TIME '01:02:03.456'")
    rows = cur.fetchall()

    assert rows[0][0] == time(1, 2, 3, 456000)


def test_null_time(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT CAST(NULL AS TIME)")
    rows = cur.fetchall()

    assert rows[0][0] is None


def test_time_with_time_zone_negative_offset(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT TIME '01:02:03.456 -08:00'")
    rows = cur.fetchall()

    tz = timezone(-timedelta(hours=8, minutes=0))

    assert rows[0][0] == time(1, 2, 3, 456000, tzinfo=tz)


def test_time_with_time_zone_positive_offset(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT TIME '01:02:03.456 +08:00'")
    rows = cur.fetchall()

    tz = timezone(timedelta(hours=8, minutes=0))

    assert rows[0][0] == time(1, 2, 3, 456000, tzinfo=tz)


def test_null_date_with_time_zone(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    cur.execute("SELECT CAST(NULL AS TIME WITH TIME ZONE)")
    rows = cur.fetchall()

    assert rows[0][0] is None


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


def test_array_none_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = [None, None]

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params

    cur.execute("SELECT TYPEOF(?)", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == "array(unknown)"


def test_array_none_and_another_type_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = [None, 1]

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params

    cur.execute("SELECT TYPEOF(?)", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == "array(integer)"


def test_array_timestamp_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = [datetime(2020, 1, 1, 0, 0, 0), datetime(2020, 1, 2, 0, 0, 0)]

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params

    cur.execute("SELECT TYPEOF(?)", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == "array(timestamp(6))"


def test_array_timestamp_with_timezone_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = [datetime(2020, 1, 1, 0, 0, 0, tzinfo=pytz.utc), datetime(2020, 1, 2, 0, 0, 0, tzinfo=pytz.utc)]

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params

    cur.execute("SELECT TYPEOF(?)", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == "array(timestamp(6) with time zone)"


def test_dict_query_param(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT ?", params=({"foo": "bar"},))
    rows = cur.fetchall()

    assert rows[0][0] == {"foo": "bar"}

    cur.execute("SELECT TYPEOF(?)", params=({"foo": "bar"},))
    rows = cur.fetchall()

    assert rows[0][0] == "map(varchar(3), varchar(3))"


def test_dict_timestamp_query_param_types(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)

    params = {"foo": datetime(2020, 1, 1, 16, 43, 22, 320000)}
    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


def test_boolean_query_param(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT ?", params=(True,))
    rows = cur.fetchall()

    assert rows[0][0] is True

    cur.execute("SELECT ?", params=(False,))
    rows = cur.fetchall()

    assert rows[0][0] is False


def test_row(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)
    params = (1, Decimal("2.0"), datetime(2020, 1, 1, 0, 0, 0))
    cur.execute("SELECT ?", (params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


def test_nested_row(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)
    params = ((1, "test", Decimal("3.1")), Decimal("2.0"), datetime(2020, 1, 1, 0, 0, 0))
    cur.execute("SELECT ?", (params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


def test_named_row(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)
    cur.execute("SELECT CAST(ROW(1, 2e0) AS ROW(x BIGINT, y DOUBLE))")
    rows = cur.fetchall()

    assert rows[0][0] == (1, 2.0)


def test_float_query_param(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT ?", params=(1.1,))
    rows = cur.fetchall()

    assert cur.description[0][1] == "double"
    assert rows[0][0] == 1.1


def test_float_nan_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)
    cur.execute("SELECT ?", params=(float("nan"),))
    rows = cur.fetchall()

    assert cur.description[0][1] == "double"
    assert isinstance(rows[0][0], float)
    assert math.isnan(rows[0][0])


def test_float_inf_query_param(trino_connection):
    cur = trino_connection.cursor(experimental_python_types=True)
    cur.execute("SELECT ?", params=(float("inf"),))
    rows = cur.fetchall()

    assert rows[0][0] == float("inf")

    cur.execute("SELECT ?", params=(float("-inf"),))
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
    cur.fetchone()
    cur.cancel()  # would raise an exception if cancel fails

    cur = trino_connection.cursor()
    with pytest.raises(Exception) as cancel_error:
        cur.cancel()
    assert "Cancel query failed; no running query" in str(cancel_error.value)


def test_close_cursor(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM tpch.sf1.customer")
    cur.fetchone()
    cur.close()  # would raise an exception if cancel fails

    cur = trino_connection.cursor()
    with pytest.raises(Exception) as cancel_error:
        cur.close()
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


@pytest.mark.skipif(trino_version() == '351', reason="Autocommit behaves "
                                                     "differently in older Trino versions")
def test_transaction_autocommit(trino_connection_in_autocommit):
    with trino_connection_in_autocommit as connection:
        connection.start_transaction()
        cur = connection.cursor()
        with pytest.raises(TrinoUserError) as transaction_error:
            cur.execute(
                """
                CREATE TABLE memory.default.nation
                AS SELECT * from tpch.tiny.nation
                """)
            cur.fetchall()
        assert "Catalog only supports writes using autocommit: memory" \
               in str(transaction_error.value)


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


def test_info_uri(trino_connection):
    cur = trino_connection.cursor()
    assert cur.info_uri is None
    cur.execute('SELECT * FROM system.runtime.nodes')
    assert cur.info_uri is not None
    assert cur._query.query_id in cur.info_uri
    cur.fetchall()
    assert cur.info_uri is not None
    assert cur._query.query_id in cur.info_uri


def test_client_tags_single_tag(run_trino):
    client_tags = ["foo"]
    query_client_tags = retrieve_client_tags_from_query(run_trino, client_tags)
    assert query_client_tags == client_tags


def test_client_tags_multiple_tags(run_trino):
    client_tags = ["foo", "bar"]
    query_client_tags = retrieve_client_tags_from_query(run_trino, client_tags)
    assert query_client_tags == client_tags


def test_client_tags_special_characters(run_trino):
    client_tags = ["foo %20", "bar=test"]
    query_client_tags = retrieve_client_tags_from_query(run_trino, client_tags)
    assert query_client_tags == client_tags


def retrieve_client_tags_from_query(run_trino, client_tags):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        client_tags=client_tags,
    )

    cur = trino_connection.cursor()
    cur.execute('SELECT 1')
    cur.fetchall()

    api_url = "http://" + trino_connection.host + ":" + str(trino_connection.port)
    query_info = requests.post(api_url + "/ui/login", data={
        "username": "admin",
        "password": "",
        "redirectPath": api_url + '/ui/api/query/' + cur._query.query_id
    }).json()

    query_client_tags = query_info['session']['clientTags']
    return query_client_tags


@pytest.mark.skipif(trino_version() == '351', reason="current_catalog not supported in older Trino versions")
def test_use_catalog_schema(trino_connection):
    cur = trino_connection.cursor()
    cur.execute('SELECT current_catalog, current_schema')
    result = cur.fetchall()
    assert result[0][0] is None
    assert result[0][1] is None

    cur.execute('USE tpch.tiny')
    cur.fetchall()
    cur.execute('SELECT current_catalog, current_schema')
    result = cur.fetchall()
    assert result[0][0] == 'tpch'
    assert result[0][1] == 'tiny'

    cur.execute('USE tpcds.sf1')
    cur.fetchall()
    cur.execute('SELECT current_catalog, current_schema')
    result = cur.fetchall()
    assert result[0][0] == 'tpcds'
    assert result[0][1] == 'sf1'


@pytest.mark.skipif(trino_version() == '351', reason="current_catalog not supported in older Trino versions")
def test_use_schema(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host, port=port, user="test", source="test", catalog="tpch", max_attempts=1
    )
    cur = trino_connection.cursor()
    cur.execute('SELECT current_catalog, current_schema')
    result = cur.fetchall()
    assert result[0][0] == 'tpch'
    assert result[0][1] is None

    cur.execute('USE tiny')
    cur.fetchall()
    cur.execute('SELECT current_catalog, current_schema')
    result = cur.fetchall()
    assert result[0][0] == 'tpch'
    assert result[0][1] == 'tiny'

    cur.execute('USE sf1')
    cur.fetchall()
    cur.execute('SELECT current_catalog, current_schema')
    result = cur.fetchall()
    assert result[0][0] == 'tpch'
    assert result[0][1] == 'sf1'


@pytest.mark.skipif(trino_version() == '351', reason="Newer Trino versions return the system role")
def test_set_role_trino_higher_351(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch"
    )
    cur = trino_connection.cursor()
    cur.execute('SHOW TABLES FROM information_schema')
    cur.fetchall()
    assert cur._request._client_session.roles == {}

    cur.execute("SET ROLE ALL")
    cur.fetchall()
    assert_role_headers(cur, "system=ALL")


@pytest.mark.skipif(trino_version() != '351', reason="Trino 351 returns the role for the current catalog")
def test_set_role_trino_351(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch"
    )
    cur = trino_connection.cursor()
    cur.execute('SHOW TABLES FROM information_schema')
    cur.fetchall()
    assert cur._request._client_session.roles == {}

    cur.execute("SET ROLE ALL")
    cur.fetchall()
    assert_role_headers(cur, "tpch=ALL")


@pytest.mark.skipif(trino_version() == '351', reason="Newer Trino versions return the system role")
def test_set_role_in_connection_trino_higher_351(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch", roles={"system": "ALL"}
    )
    cur = trino_connection.cursor()
    cur.execute('SHOW TABLES FROM information_schema')
    cur.fetchall()
    assert_role_headers(cur, "system=ALL")


def assert_role_headers(cursor, expected_header):
    assert cursor._request.http_headers[constants.HEADER_ROLE] == expected_header


def test_prepared_statements(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch",
    )
    cur = trino_connection.cursor()

    # Implicit prepared statements must work and deallocate statements on finish
    assert cur._request._client_session.prepared_statements == {}
    cur.execute('SELECT count(1) FROM tpch.tiny.nation WHERE nationkey = ?', (1,))
    result = cur.fetchall()
    assert result[0][0] == 1
    assert cur._request._client_session.prepared_statements == {}

    # Explicit prepared statements must also work
    cur.execute('PREPARE test_prepared_statements FROM SELECT count(1) FROM tpch.tiny.nation WHERE nationkey = ?')
    cur.fetchall()
    assert 'test_prepared_statements' in cur._request._client_session.prepared_statements
    cur.execute('EXECUTE test_prepared_statements USING 1')
    cur.fetchall()
    assert result[0][0] == 1

    # An implicit prepared statement must not deallocate explicit statements
    cur.execute('SELECT count(1) FROM tpch.tiny.nation WHERE nationkey = ?', (1,))
    result = cur.fetchall()
    assert result[0][0] == 1
    assert 'test_prepared_statements' in cur._request._client_session.prepared_statements

    assert 'test_prepared_statements' in cur._request._client_session.prepared_statements
    cur.execute('DEALLOCATE PREPARE test_prepared_statements')
    cur.fetchall()
    assert cur._request._client_session.prepared_statements == {}
