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
import time as t
import uuid
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from typing import Tuple
from zoneinfo import ZoneInfo

import pytest
import requests
from tzlocal import get_localzone_name  # type: ignore

import trino
from tests.integration.conftest import trino_version
from trino import constants
from trino.dbapi import Cursor
from trino.dbapi import DescribeOutput
from trino.dbapi import TimeBoundLRUCache
from trino.exceptions import NotSupportedError
from trino.exceptions import TrinoQueryError
from trino.exceptions import TrinoUserError
from trino.transaction import IsolationLevel


@pytest.fixture
def trino_connection(run_trino):
    _, host, port = run_trino

    yield trino.dbapi.Connection(host=host, port=port, user="test", source="test", max_attempts=1)


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
    assert cur.query_id is not None
    assert cur.query == "SELECT * FROM system.runtime.nodes"
    assert cur.stats is not None


def test_select_query_result_iteration(trino_connection):
    cur0 = trino_connection.cursor()
    cur0.execute("SELECT custkey FROM tpch.sf1.customer LIMIT 10")
    rows0 = cur0.genall()

    cur1 = trino_connection.cursor()
    cur1.execute("SELECT custkey FROM tpch.sf1.customer LIMIT 10")
    rows1 = cur1.fetchall()

    assert len(list(rows0)) == len(rows1)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_select_query_result_iteration_statement_params(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

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
        params=(3,),  # expecting all the rows with id >= 3
    )
    rows = cur.fetchall()
    assert len(rows) == 3
    assert [3, "three", "c"] in rows
    assert [4, "four", "d"] in rows
    assert [5, "five", "e"] in rows


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_none_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)
    cur.execute("SELECT ?", params=(None,))
    rows = cur.fetchall()

    assert rows[0][0] is None
    assert_cursor_description(cur, trino_type="unknown")


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_string_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    cur.execute("SELECT ?", params=("six'",))
    rows = cur.fetchall()

    assert rows[0][0] == "six'"
    assert_cursor_description(cur, trino_type="varchar(4)", size=4)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_execute_many(legacy_prepared_statements, run_trino):

    try:
        cur = get_cursor(legacy_prepared_statements, run_trino)
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
    finally:
        cur = get_cursor(legacy_prepared_statements, run_trino)
        cur.execute("DROP TABLE IF EXISTS memory.default.test_execute_many")


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_execute_many_without_params(legacy_prepared_statements, run_trino):

    try:
        cur = get_cursor(legacy_prepared_statements, run_trino)
        cur.execute("CREATE TABLE memory.default.test_execute_many_without_param (value varchar)")
        cur.fetchall()
        with pytest.raises(TrinoUserError) as e:
            cur.executemany("INSERT INTO memory.default.test_execute_many_without_param (value) VALUES (?)", [])
            cur.fetchall()
        assert "Incorrect number of parameters: expected 1 but found 0" in str(e.value)
    finally:
        cur = get_cursor(legacy_prepared_statements, run_trino)
        cur.execute("DROP TABLE IF EXISTS memory.default.test_execute_many_without_param")


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_execute_many_select(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)
    with pytest.raises(NotSupportedError) as e:
        cur.executemany("SELECT ?, ?", [(1, "value1"), (2, "value2")])
    assert "Query must return update type" in str(e.value)


@pytest.mark.parametrize("connection_legacy_primitive_types", [None, True, False])
@pytest.mark.parametrize("cursor_legacy_primitive_types", [None, True, False])
def test_legacy_primitive_types_with_connection_and_cursor(
    connection_legacy_primitive_types, cursor_legacy_primitive_types, run_trino
):
    _, host, port = run_trino

    connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        legacy_primitive_types=connection_legacy_primitive_types,
    )

    cur = connection.cursor(legacy_primitive_types=cursor_legacy_primitive_types)

    # If legacy_primitive_types is passed to cursor, take value from it.
    # If not, take value from legacy_primitive_types passed to connection.
    # If legacy_primitive_types is not passed to cursor nor connection, default to False.
    if cursor_legacy_primitive_types is not None:
        expected_legacy_primitive_types = cursor_legacy_primitive_types
    elif connection_legacy_primitive_types is not None:
        expected_legacy_primitive_types = connection_legacy_primitive_types
    else:
        expected_legacy_primitive_types = False

    test_query = """
    SELECT
        DECIMAL '0.142857',
        DATE '2018-01-01',
        TIMESTAMP '2019-01-01 00:00:00.000+01:00',
        TIMESTAMP '2019-01-01 00:00:00.000 UTC',
        TIMESTAMP '2019-01-01 00:00:00.000',
        TIME '00:00:00.000'
    """
    # Check values which cannot be represented by Python types
    if expected_legacy_primitive_types:
        test_query += """
        ,DATE '-2001-08-22'
        """
    cur.execute(test_query)
    rows = cur.fetchall()

    if not expected_legacy_primitive_types:
        assert len(rows[0]) == 6
        assert rows[0][0] == Decimal("0.142857")
        assert rows[0][1] == date(2018, 1, 1)
        assert rows[0][2] == datetime(2019, 1, 1, tzinfo=timezone(timedelta(hours=1)))
        assert rows[0][3] == datetime(2019, 1, 1, tzinfo=ZoneInfo("UTC"))
        assert rows[0][4] == datetime(2019, 1, 1)
        assert rows[0][5] == time(0, 0, 0, 0)
    else:
        for value in rows[0]:
            assert isinstance(value, str)

        assert len(rows[0]) == 7
        assert rows[0][0] == "0.142857"
        assert rows[0][1] == "2018-01-01"
        assert rows[0][2] == "2019-01-01 00:00:00.000 +01:00"
        assert rows[0][3] == "2019-01-01 00:00:00.000 UTC"
        assert rows[0][4] == "2019-01-01 00:00:00.000"
        assert rows[0][5] == "00:00:00.000"
        assert rows[0][6] == "-2001-08-22"


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_decimal_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    cur.execute("SELECT ?", params=(Decimal("1112.142857"),))
    rows = cur.fetchall()

    assert rows[0][0] == Decimal("1112.142857")
    assert_cursor_description(cur, trino_type="decimal(10, 6)", precision=10, scale=6)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_decimal_scientific_notation_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    cur.execute("SELECT ?", params=(Decimal("0E-10"),))
    rows = cur.fetchall()

    assert rows[0][0] == Decimal("0E-10")
    assert_cursor_description(cur, trino_type="decimal(10, 10)", precision=10, scale=10)

    # Ensure we don't convert to floats
    assert Decimal("0.1") == Decimal("1E-1") != 0.1

    cur.execute("SELECT ?", params=(Decimal("1E-1"),))
    rows = cur.fetchall()

    assert rows[0][0] == Decimal("1E-1")
    assert_cursor_description(cur, trino_type="decimal(1, 1)", precision=1, scale=1)


def test_null_decimal(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT CAST(NULL AS DECIMAL)")
    rows = cur.fetchall()

    assert rows[0][0] is None
    assert_cursor_description(cur, trino_type="decimal(38, 0)", precision=38, scale=0)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_biggest_decimal(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = Decimal("99999999999999999999999999999999999999")
    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert_cursor_description(cur, trino_type="decimal(38, 0)", precision=38, scale=0)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_smallest_decimal(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = Decimal("-99999999999999999999999999999999999999")
    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert_cursor_description(cur, trino_type="decimal(38, 0)", precision=38, scale=0)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_highest_precision_decimal(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = Decimal("0.99999999999999999999999999999999999999")
    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert_cursor_description(cur, trino_type="decimal(38, 38)", precision=38, scale=38)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_datetime_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = datetime(2020, 1, 1, 16, 43, 22, 320000)

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert_cursor_description(cur, trino_type="timestamp(6)", precision=6)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_datetime_with_utc_time_zone_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=ZoneInfo("UTC"))

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert_cursor_description(cur, trino_type="timestamp(6) with time zone", precision=6)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_datetime_with_numeric_offset_time_zone_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    tz = timezone(-timedelta(hours=5, minutes=30))

    params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=tz)

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert_cursor_description(cur, trino_type="timestamp(6) with time zone", precision=6)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_datetime_with_named_time_zone_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=ZoneInfo("America/Los_Angeles"))

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert_cursor_description(cur, trino_type="timestamp(6) with time zone", precision=6)


def test_datetime_with_trailing_zeros(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT TIMESTAMP '2001-08-22 03:04:05.321000'")
    rows = cur.fetchall()

    assert rows[0][0] == datetime.strptime("2001-08-22 03:04:05.321000", "%Y-%m-%d %H:%M:%S.%f")
    assert_cursor_description(cur, trino_type="timestamp(6)", precision=6)


def test_null_datetime_with_time_zone(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT CAST(NULL AS TIMESTAMP WITH TIME ZONE)")
    rows = cur.fetchall()

    assert rows[0][0] is None
    assert_cursor_description(cur, trino_type="timestamp(3) with time zone", precision=3)


def test_datetime_with_time_zone_numeric_offset(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT TIMESTAMP '2001-08-22 03:04:05.321 -08:00'")
    rows = cur.fetchall()

    assert rows[0][0] == datetime.strptime("2001-08-22 03:04:05.321 -08:00", "%Y-%m-%d %H:%M:%S.%f %z")
    assert_cursor_description(cur, trino_type="timestamp(3) with time zone", precision=3)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_datetimes_with_time_zone_in_dst_gap_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    # This is a datetime that lies within a DST transition and not actually exists.
    params = datetime(2021, 3, 28, 2, 30, 0, tzinfo=ZoneInfo("Europe/Brussels"))
    with pytest.raises(trino.exceptions.TrinoUserError):
        cur.execute("SELECT ?", params=(params,))
        cur.fetchall()


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
@pytest.mark.parametrize("fold", [0, 1])
def test_doubled_datetimes(fold, legacy_prepared_statements, run_trino):
    # Trino doesn't distinguish between doubled datetimes that lie within a DST transition.
    # See also https://github.com/trinodb/trino/issues/5781
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = datetime(2002, 10, 27, 1, 30, 0, tzinfo=ZoneInfo("US/Eastern"), fold=fold)

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == datetime(2002, 10, 27, 1, 30, 0, tzinfo=ZoneInfo("US/Eastern"))


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_date_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = datetime(2020, 1, 1, 0, 0, 0).date()

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert_cursor_description(cur, trino_type="date")


def test_null_date(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT CAST(NULL AS DATE)")
    rows = cur.fetchall()

    assert rows[0][0] is None
    assert_cursor_description(cur, trino_type="date")


def test_unsupported_python_dates(trino_connection):
    cur = trino_connection.cursor()

    # dates below python min (1-1-1) or above max date (9999-12-31) are not supported
    for unsupported_date in [
        "-0001-01-01",
        "0000-01-01",
        "10000-01-01",
        "-4999999-01-01",  # Trino min date
        "5000000-12-31",  # Trino max date
    ]:
        with pytest.raises(trino.exceptions.TrinoDataError):
            cur.execute(f"SELECT DATE '{unsupported_date}'")
            cur.fetchall()


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_supported_special_dates_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

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


def test_char(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT CHAR 'trino'")
    rows = cur.fetchall()

    assert rows[0][0] == "trino"
    assert_cursor_description(cur, trino_type="char(5)", size=5)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_time_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = time(12, 3, 44, 333000)

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params
    assert_cursor_description(cur, trino_type="time(6)", precision=6)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_time_with_named_time_zone_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = time(16, 43, 22, 320000, tzinfo=ZoneInfo("Asia/Shanghai"))

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    # Asia/Shanghai
    assert rows[0][0].tzinfo == timezone(timedelta(seconds=28800))


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_time_with_numeric_offset_time_zone_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    tz = timezone(-timedelta(hours=8, minutes=0))
    params = time(16, 43, 22, 320000, tzinfo=tz)

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


def test_time(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT TIME '01:02:03.456'")
    rows = cur.fetchall()

    assert rows[0][0] == time(1, 2, 3, 456000)
    assert_cursor_description(cur, trino_type="time(3)", precision=3)


def test_null_time(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT CAST(NULL AS TIME)")
    rows = cur.fetchall()

    assert rows[0][0] is None
    assert_cursor_description(cur, trino_type="time(3)", precision=3)


def test_time_with_time_zone_negative_offset(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT TIME '01:02:03.456 -08:00'")
    rows = cur.fetchall()

    tz = timezone(-timedelta(hours=8, minutes=0))

    assert rows[0][0] == time(1, 2, 3, 456000, tzinfo=tz)
    assert_cursor_description(cur, trino_type="time(3) with time zone", precision=3)


def test_time_with_time_zone_positive_offset(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT TIME '01:02:03.456 +08:00'")
    rows = cur.fetchall()

    tz = timezone(timedelta(hours=8, minutes=0))

    assert rows[0][0] == time(1, 2, 3, 456000, tzinfo=tz)
    assert_cursor_description(cur, trino_type="time(3) with time zone", precision=3)


def test_null_date_with_time_zone(trino_connection):
    cur = trino_connection.cursor()

    cur.execute("SELECT CAST(NULL AS TIME WITH TIME ZONE)")
    rows = cur.fetchall()

    assert rows[0][0] is None
    assert_cursor_description(cur, trino_type="time(3) with time zone", precision=3)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
@pytest.mark.parametrize(
    "binary_input",
    [
        bytearray("a", "utf-8"),
        bytearray("a", "ascii"),
        bytearray(b"\x00\x00\x00\x00"),
        bytearray(4),
        bytearray([1, 2, 3]),
    ],
)
def test_binary_query_param(binary_input, legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    cur.execute("SELECT ?", params=(binary_input,))
    rows = cur.fetchall()

    assert rows[0][0] == binary_input


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_array_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    cur.execute("SELECT ?", params=([1, 2, 3],))
    rows = cur.fetchall()

    assert rows[0][0] == [1, 2, 3]

    cur.execute("SELECT ?", params=([[1, 2, 3], [4, 5, 6]],))
    rows = cur.fetchall()

    assert rows[0][0] == [[1, 2, 3], [4, 5, 6]]

    cur.execute("SELECT TYPEOF(?)", params=([1, 2, 3],))
    rows = cur.fetchall()

    assert rows[0][0] == "array(integer)"


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_array_none_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = [None, None]

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params

    cur.execute("SELECT TYPEOF(?)", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == "array(unknown)"


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_array_none_and_another_type_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = [None, 1]

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params

    cur.execute("SELECT TYPEOF(?)", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == "array(integer)"


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_array_timestamp_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = [datetime(2020, 1, 1, 0, 0, 0), datetime(2020, 1, 2, 0, 0, 0)]

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params

    cur.execute("SELECT TYPEOF(?)", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == "array(timestamp(6))"


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_array_timestamp_with_timezone_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = [
        datetime(2020, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("UTC")),
        datetime(2020, 1, 2, 0, 0, 0, tzinfo=ZoneInfo("UTC")),
    ]

    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params

    cur.execute("SELECT TYPEOF(?)", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == "array(timestamp(6) with time zone)"


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_dict_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    cur.execute("SELECT ?", params=({"foo": "bar"},))
    rows = cur.fetchall()

    assert rows[0][0] == {"foo": "bar"}

    cur.execute("SELECT TYPEOF(?)", params=({"foo": "bar"},))
    rows = cur.fetchall()

    assert rows[0][0] == "map(varchar(3), varchar(3))"


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_dict_timestamp_query_param_types(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    params = {"foo": datetime(2020, 1, 1, 16, 43, 22, 320000)}
    cur.execute("SELECT ?", params=(params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_boolean_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    cur.execute("SELECT ?", params=(True,))
    rows = cur.fetchall()

    assert rows[0][0] is True

    cur.execute("SELECT ?", params=(False,))
    rows = cur.fetchall()

    assert rows[0][0] is False


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_row(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)
    params = (1, Decimal("2.0"), datetime(2020, 1, 1, 0, 0, 0))
    cur.execute("SELECT ?", (params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_nested_row(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)
    params = ((1, "test", Decimal("3.1")), Decimal("2.0"), datetime(2020, 1, 1, 0, 0, 0))
    cur.execute("SELECT ?", (params,))
    rows = cur.fetchall()

    assert rows[0][0] == params


def test_named_row(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT CAST(ROW(1, 2e0) AS ROW(x BIGINT, y DOUBLE))")
    rows = cur.fetchall()

    assert rows[0][0] == (1, 2.0)
    assert rows[0][0][0] == 1
    assert rows[0][0][1] == 2.0
    assert rows[0][0].x == 1
    assert rows[0][0].y == 2.0

    assert rows[0][0].__annotations__["names"] == ["x", "y"]
    assert rows[0][0].__annotations__["types"] == ["bigint", "double"]


def test_named_row_duplicate_names(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT CAST(ROW(1, 2e0) AS ROW(x BIGINT, x DOUBLE))")
    rows = cur.fetchall()

    assert rows[0][0] == (1, 2.0)
    with pytest.raises(ValueError, match="Ambiguous row field reference: x"):
        rows[0][0].x

    assert rows[0][0].__annotations__["names"] == ["x", "x"]
    assert rows[0][0].__annotations__["types"] == ["bigint", "double"]
    assert str(rows[0][0]) == "(1, 2.0)"


def test_nested_named_row(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT CAST(ROW(DECIMAL '2.3', ROW(1, 'test')) AS ROW(x DECIMAL(3,2), y ROW(x BIGINT, y VARCHAR)))")
    rows = cur.fetchall()

    assert rows[0][0] == (Decimal("2.3"), (1, "test"))
    assert rows[0][0][0] == Decimal("2.3")
    assert rows[0][0][1] == (1, "test")
    assert rows[0][0][1][0] == 1
    assert rows[0][0][1][1] == "test"
    assert rows[0][0].x == Decimal("2.3")
    assert rows[0][0].y.x == 1
    assert rows[0][0].y.y == "test"

    assert rows[0][0].__annotations__["names"] == ["x", "y"]
    assert rows[0][0].__annotations__["types"] == ["decimal", "row"]

    assert rows[0][0].y.__annotations__["names"] == ["x", "y"]
    assert rows[0][0].y.__annotations__["types"] == ["bigint", "varchar"]
    assert str(rows[0][0]) == "(x: Decimal('2.30'), y: (x: 1, y: 'test'))"


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_float_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)
    cur.execute("SELECT ?", params=(1.1,))
    rows = cur.fetchall()

    assert_cursor_description(cur, trino_type="double")
    assert rows[0][0] == 1.1


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_float_nan_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)
    cur.execute("SELECT ?", params=(float("nan"),))
    rows = cur.fetchall()

    assert_cursor_description(cur, trino_type="double")
    assert isinstance(rows[0][0], float)
    assert math.isnan(rows[0][0])


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_float_inf_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)
    cur.execute("SELECT ?", params=(float("inf"),))
    rows = cur.fetchall()

    assert_cursor_description(cur, trino_type="double")
    assert rows[0][0] == float("inf")

    cur.execute("SELECT ?", params=(float("-inf"),))
    rows = cur.fetchall()

    assert rows[0][0] == float("-inf")


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_int_query_param(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)
    cur.execute("SELECT ?", params=(3,))
    rows = cur.fetchall()

    assert rows[0][0] == 3
    assert_cursor_description(cur, trino_type="integer")

    cur.execute("SELECT ?", params=(9223372036854775807,))
    rows = cur.fetchall()

    assert rows[0][0] == 9223372036854775807
    assert_cursor_description(cur, trino_type="bigint")


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
@pytest.mark.parametrize(
    "params",
    [
        "NOT A LIST OR TUPPLE",
        {"invalid", "params"},
        object,
    ],
)
def test_select_query_invalid_params(params, legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)
    with pytest.raises(AssertionError):
        cur.execute("SELECT ?", params=params)


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


def test_execute_chaining(trino_connection):
    cur = trino_connection.cursor()
    assert cur.execute("SELECT 1").fetchone()[0] == 1


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


def test_fetch_cursor(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
    for _ in range(100):
        cur.fetchone()
    assert len(cur.fetchmany(400)) == 400
    assert len(cur.fetchall()) == 500


def test_cancel_query(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM tpch.sf1.customer")
    cur.fetchone()
    cur.cancel()  # would raise an exception if cancel fails

    # verify that it doesn't fail in the absence of a previously running query
    cur = trino_connection.cursor()
    cur.cancel()


def test_close_cursor(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM tpch.sf1.customer")
    cur.fetchone()
    cur.close()  # would raise an exception if cancel fails

    # verify that it doesn't fail in the absence of a previously running query
    cur = trino_connection.cursor()
    cur.close()


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


@pytest.mark.skipif(trino_version() == "351", reason="Autocommit behaves " "differently in older Trino versions")
def test_transaction_autocommit(trino_connection_in_autocommit):
    with trino_connection_in_autocommit as connection:
        connection.start_transaction()
        cur = connection.cursor()
        with pytest.raises(TrinoUserError) as transaction_error:
            cur.execute(
                """
                CREATE TABLE memory.default.nation
                AS SELECT * from tpch.tiny.nation
                """
            )
            cur.fetchall()
        assert "Catalog only supports writes using autocommit: memory" in str(transaction_error.value)


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_invalid_query_throws_correct_error(legacy_prepared_statements, run_trino):
    """Tests that an invalid query raises the correct exception"""
    cur = get_cursor(legacy_prepared_statements, run_trino)
    with pytest.raises(TrinoQueryError):
        cur.execute(
            """
            SELECT * FRMO foo WHERE x = ?;
            """,
            params=(3,),
        )


def test_eager_loading_cursor_description(trino_connection):
    description_expected = [
        ("node_id", "varchar", None, None, None, None, None),
        ("http_uri", "varchar", None, None, None, None, None),
        ("node_version", "varchar", None, None, None, None, None),
        ("coordinator", "boolean", None, None, None, None, None),
        ("state", "varchar", None, None, None, None, None),
    ]
    cur = trino_connection.cursor()
    cur.execute("SELECT * FROM system.runtime.nodes")
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
    cur.execute("SELECT * FROM system.runtime.nodes")
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
    cur.execute("SELECT 1")
    cur.fetchall()

    api_url = "http://" + trino_connection.host + ":" + str(trino_connection.port)
    query_info = requests.post(
        api_url + "/ui/login",
        data={"username": "admin", "password": "", "redirectPath": api_url + "/ui/api/query/" + cur._query.query_id},
    ).json()

    query_client_tags = query_info["session"]["clientTags"]
    return query_client_tags


@pytest.mark.skipif(trino_version() == "351", reason="current_catalog not supported in older Trino versions")
def test_use_catalog_schema(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT current_catalog, current_schema")
    result = cur.fetchall()
    assert result[0][0] is None
    assert result[0][1] is None

    cur.execute("USE tpch.tiny")
    cur.fetchall()
    cur.execute("SELECT current_catalog, current_schema")
    result = cur.fetchall()
    assert result[0][0] == "tpch"
    assert result[0][1] == "tiny"

    cur.execute("USE tpcds.sf1")
    cur.fetchall()
    cur.execute("SELECT current_catalog, current_schema")
    result = cur.fetchall()
    assert result[0][0] == "tpcds"
    assert result[0][1] == "sf1"


@pytest.mark.skipif(trino_version() == "351", reason="current_catalog not supported in older Trino versions")
def test_use_schema(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host, port=port, user="test", source="test", catalog="tpch", max_attempts=1
    )
    cur = trino_connection.cursor()
    cur.execute("SELECT current_catalog, current_schema")
    result = cur.fetchall()
    assert result[0][0] == "tpch"
    assert result[0][1] is None

    cur.execute("USE tiny")
    cur.fetchall()
    cur.execute("SELECT current_catalog, current_schema")
    result = cur.fetchall()
    assert result[0][0] == "tpch"
    assert result[0][1] == "tiny"

    cur.execute("USE sf1")
    cur.fetchall()
    cur.execute("SELECT current_catalog, current_schema")
    result = cur.fetchall()
    assert result[0][0] == "tpch"
    assert result[0][1] == "sf1"


def test_set_role(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(host=host, port=port, user="test", catalog="tpch")
    cur = trino_connection.cursor()
    cur.execute("SHOW TABLES FROM information_schema")
    cur.fetchall()
    assert cur._request._client_session.roles == {}

    cur.execute("SET ROLE ALL")
    cur.fetchall()
    if trino_version() == "351":
        assert_role_headers(cur, "tpch=ALL")
    else:
        # Newer Trino versions return the system role
        assert_role_headers(cur, "system=ALL")


def test_set_role_in_connection(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch", roles={"system": "ALL"}
    )
    cur = trino_connection.cursor()
    cur.execute("SHOW TABLES FROM information_schema")
    cur.fetchall()
    assert_role_headers(cur, "system=ALL")


def test_set_system_role_in_connection(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(host=host, port=port, user="test", catalog="tpch", roles="ALL")
    cur = trino_connection.cursor()
    cur.execute("SHOW TABLES FROM information_schema")
    cur.fetchall()
    assert_role_headers(cur, "system=ALL")


def assert_role_headers(cursor, expected_header):
    assert cursor._request.http_headers[constants.HEADER_ROLE] == expected_header


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            None, marks=pytest.mark.skipif(trino_version() > "417", reason="This would use EXECUTE IMMEDIATE")
        ),
    ],
)
def test_prepared_statements(legacy_prepared_statements, run_trino):
    cur = get_cursor(legacy_prepared_statements, run_trino)

    # Implicit prepared statements must work and deallocate statements on finish
    assert cur._request._client_session.prepared_statements == {}
    cur.execute("SELECT count(1) FROM tpch.tiny.nation WHERE nationkey = ?", (1,))
    result = cur.fetchall()
    assert result[0][0] == 1
    assert cur._request._client_session.prepared_statements == {}

    # Explicit prepared statements must also work
    cur.execute("PREPARE test_prepared_statements FROM SELECT count(1) FROM tpch.tiny.nation WHERE nationkey = ?")
    cur.fetchall()
    assert "test_prepared_statements" in cur._request._client_session.prepared_statements
    cur.execute("EXECUTE test_prepared_statements USING 1")
    cur.fetchall()
    assert result[0][0] == 1

    # An implicit prepared statement must not deallocate explicit statements
    cur.execute("SELECT count(1) FROM tpch.tiny.nation WHERE nationkey = ?", (1,))
    result = cur.fetchall()
    assert result[0][0] == 1
    assert "test_prepared_statements" in cur._request._client_session.prepared_statements

    assert "test_prepared_statements" in cur._request._client_session.prepared_statements
    cur.execute("DEALLOCATE PREPARE test_prepared_statements")
    cur.fetchall()
    assert cur._request._client_session.prepared_statements == {}


def test_set_timezone_in_connection(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host, port=port, user="test", catalog="tpch", timezone="Europe/Brussels"
    )
    cur = trino_connection.cursor()
    cur.execute("SELECT current_timezone()")
    res = cur.fetchall()
    assert res[0][0] == "Europe/Brussels"


def test_connection_without_timezone(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(host=host, port=port, user="test", catalog="tpch")
    cur = trino_connection.cursor()
    cur.execute("SELECT current_timezone()")
    res = cur.fetchall()
    session_tz = res[0][0]
    localzone = get_localzone_name()
    assert session_tz == localzone or (
        session_tz == "UTC" and localzone == "Etc/UTC"
    )  # Workaround for difference between Trino timezone and tzlocal for UTC


def test_describe(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        catalog="tpch",
    )
    cur = trino_connection.cursor()

    result = cur.describe("SELECT 1, DECIMAL '1.0' as a")

    assert result == [
        DescribeOutput(name="_col0", catalog="", schema="", table="", type="integer", type_size=4, aliased=False),
        DescribeOutput(name="a", catalog="", schema="", table="", type="decimal(2,1)", type_size=8, aliased=True),
    ]


def test_describe_table_query(run_trino):
    _, host, port = run_trino

    trino_connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        catalog="tpch",
    )
    cur = trino_connection.cursor()

    result = cur.describe("SELECT * from tpch.tiny.nation")

    assert result == [
        DescribeOutput(
            name="nationkey",
            catalog="tpch",
            schema="tiny",
            table="nation",
            type="bigint",
            type_size=8,
            aliased=False,
        ),
        DescribeOutput(
            name="name",
            catalog="tpch",
            schema="tiny",
            table="nation",
            type="varchar(25)",
            type_size=0,
            aliased=False,
        ),
        DescribeOutput(
            name="regionkey",
            catalog="tpch",
            schema="tiny",
            table="nation",
            type="bigint",
            type_size=8,
            aliased=False,
        ),
        DescribeOutput(
            name="comment",
            catalog="tpch",
            schema="tiny",
            table="nation",
            type="varchar(152)",
            type_size=0,
            aliased=False,
        ),
    ]


def test_rowcount_select(trino_connection):
    cur = trino_connection.cursor()
    cur.execute("SELECT 1 as a")
    cur.fetchall()
    assert cur.rowcount == -1


def test_rowcount_create_table(trino_connection):
    with _TestTable(trino_connection, "memory.default.test_rowcount_create_table", "(a varchar)") as (_, cur):
        assert cur.rowcount == -1


def test_rowcount_create_table_as_select(trino_connection):
    with _TestTable(trino_connection, "memory.default.test_rowcount_ctas", "AS SELECT 1 a UNION ALL SELECT 2") as (
        _,
        cur,
    ):
        assert cur.rowcount == 2


def test_rowcount_insert(trino_connection):
    with _TestTable(trino_connection, "memory.default.test_rowcount_ctas", "(a VARCHAR)") as (table, cur):
        cur.execute(f"INSERT INTO {table.table_name} (a) VALUES ('test')")
        assert cur.rowcount == 1


@pytest.mark.parametrize(
    "legacy_prepared_statements",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                trino_version() <= "417", reason="EXECUTE IMMEDIATE was introduced in version 418"
            ),
        ),
        None,
    ],
)
def test_prepared_statement_capability_autodetection(legacy_prepared_statements, run_trino):
    # start with an empty cache
    trino.dbapi.must_use_legacy_prepared_statements = TimeBoundLRUCache(1024, 3600)
    user_name = f"user_{t.monotonic_ns()}"

    _, host, port = run_trino
    connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user=user_name,
        legacy_prepared_statements=legacy_prepared_statements,
    )
    cur = connection.cursor()
    cur.execute("SELECT ?", [42])
    cur.fetchall()
    another = connection.cursor()
    another.execute("SELECT ?", [100])
    another.fetchall()

    verify = connection.cursor()
    rows = verify.execute("SELECT query FROM system.runtime.queries WHERE user = ?", [user_name])
    statements = [stmt for row in rows for stmt in row]
    assert statements.count("EXECUTE IMMEDIATE 'SELECT 1'") == (1 if legacy_prepared_statements is None else 0)


def get_cursor(legacy_prepared_statements, run_trino):
    _, host, port = run_trino

    connection = trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        legacy_prepared_statements=legacy_prepared_statements,
    )
    return connection.cursor()


def assert_cursor_description(cur, trino_type, size=None, precision=None, scale=None):
    assert cur.description[0][1] == trino_type
    assert cur.description[0][2] is None
    assert cur.description[0][3] is size
    assert cur.description[0][4] is precision
    assert cur.description[0][5] is scale
    assert cur.description[0][6] is None


class _TestTable:
    def __init__(self, conn, table_name_prefix, table_definition) -> None:
        self._conn = conn
        self._table_name = table_name_prefix + "_" + str(uuid.uuid4().hex)
        self._table_definition = table_definition

    def __enter__(self) -> Tuple["_TestTable", Cursor]:
        return (self, self._conn.cursor().execute(f"CREATE TABLE {self._table_name} {self._table_definition}"))

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        self._conn.cursor().execute(f"DROP TABLE {self._table_name}")

    @property
    def table_name(self):
        return self._table_name
