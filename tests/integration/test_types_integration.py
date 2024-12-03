import math
import re
import uuid
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from datetime import timezone
from datetime import tzinfo
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from dateutil.relativedelta import relativedelta

import trino
from tests.integration.conftest import trino_version


@pytest.fixture
def trino_connection(run_trino):
    _, host, port = run_trino

    yield trino.dbapi.Connection(
        host=host, port=port, user="test", source="test", max_attempts=1
    )


def test_boolean(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS BOOLEAN)", python=None) \
        .add_field(sql="false", python=False) \
        .add_field(sql="true", python=True) \
        .execute()


def test_tinyint(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS TINYINT)", python=None) \
        .add_field(sql="CAST(-128 AS TINYINT)", python=-128) \
        .add_field(sql="CAST(42 AS TINYINT)", python=42) \
        .add_field(sql="CAST(127 AS TINYINT)", python=127) \
        .execute()


def test_smallint(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS SMALLINT)", python=None) \
        .add_field(sql="CAST(-32768 AS SMALLINT)", python=-32768) \
        .add_field(sql="CAST(42 AS SMALLINT)", python=42) \
        .add_field(sql="CAST(32767 AS SMALLINT)", python=32767) \
        .execute()


def test_int(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS INTEGER)", python=None) \
        .add_field(sql="CAST(-2147483648 AS INTEGER)", python=-2147483648) \
        .add_field(sql="CAST(83648 AS INTEGER)", python=83648) \
        .add_field(sql="CAST(2147483647 AS INTEGER)", python=2147483647) \
        .execute()


def test_bigint(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS BIGINT)", python=None) \
        .add_field(sql="CAST(-9223372036854775808 AS BIGINT)", python=-9223372036854775808) \
        .add_field(sql="CAST(9223 AS BIGINT)", python=9223) \
        .add_field(sql="CAST(9223372036854775807 AS BIGINT)", python=9223372036854775807) \
        .execute()


def test_real(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS REAL)", python=None) \
        .add_field(sql="CAST('NaN' AS REAL)", python=math.nan, has_nan=True) \
        .add_field(sql="CAST('-Infinity' AS REAL)", python=-math.inf) \
        .add_field(sql="CAST(3.4028235E38 AS REAL)", python=3.4028235e+38) \
        .add_field(sql="CAST(1.4E-45 AS REAL)", python=1.4e-45) \
        .add_field(sql="CAST('Infinity' AS REAL)", python=math.inf) \
        .execute()


def test_double(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS DOUBLE)", python=None) \
        .add_field(sql="CAST('NaN' AS DOUBLE)", python=math.nan, has_nan=True) \
        .add_field(sql="CAST('-Infinity' AS DOUBLE)", python=-math.inf) \
        .add_field(sql="CAST(1.7976931348623157E308 AS DOUBLE)", python=1.7976931348623157e+308) \
        .add_field(sql="CAST(4.9E-324 AS DOUBLE)", python=5e-324) \
        .add_field(sql="CAST('Infinity' AS DOUBLE)", python=math.inf) \
        .execute()


def test_decimal(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS DECIMAL)", python=None) \
        .add_field(sql="CAST(null AS DECIMAL(38,0))", python=None) \
        .add_field(sql="DECIMAL '10.3'", python=Decimal('10.3')) \
        .add_field(sql="CAST('0.123456789123456789' AS DECIMAL(18,18))", python=Decimal('0.123456789123456789')) \
        .add_field(sql="CAST(null AS DECIMAL(18,18))", python=None) \
        .add_field(sql="CAST('234.123456789123456789' AS DECIMAL(18,4))", python=Decimal('234.1235')) \
        .add_field(sql="CAST('10.3' AS DECIMAL(38,1))", python=Decimal('10.3')) \
        .add_field(sql="CAST('0.123456789123456789' AS DECIMAL(18,2))", python=Decimal('0.12')) \
        .add_field(sql="CAST('0.3123' AS DECIMAL(38,38))", python=Decimal('0.3123')) \
        .execute()


def test_varchar(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="'aaa'", python='aaa') \
        .add_field(sql="U&'Hello winter \2603 !'", python='Hello winter °3 !') \
        .add_field(sql="CAST(null AS VARCHAR)", python=None) \
        .add_field(sql="CAST('bbb' AS VARCHAR(1))", python='b') \
        .add_field(sql="CAST(null AS VARCHAR(1))", python=None) \
        .execute()


def test_char(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST('ccc' AS CHAR)", python='c') \
        .add_field(sql="CAST('ccc' AS CHAR(5))", python='ccc  ') \
        .add_field(sql="CAST(null AS CHAR)", python=None) \
        .add_field(sql="CAST('ddd' AS CHAR(1))", python='d') \
        .add_field(sql="CAST('😂' AS CHAR(1))", python='😂') \
        .add_field(sql="CAST(null AS CHAR(1))", python=None) \
        .execute()


def test_varbinary(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="X'65683F'", python=b'eh?') \
        .add_field(sql="X''", python=b'') \
        .add_field(sql="CAST('' AS VARBINARY)", python=b'') \
        .add_field(sql="CAST(null AS VARBINARY)", python=None) \
        .execute()


def test_varbinary_failure(trino_connection):
    SqlExpectFailureTest(trino_connection) \
        .execute("CAST(42 AS VARBINARY)")


def test_json(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST('{}' AS JSON)", python='"{}"') \
        .add_field(sql="CAST('null' AS JSON)", python='"null"') \
        .add_field(sql="CAST(null AS JSON)", python=None) \
        .add_field(sql="CAST('3.14' AS JSON)", python='"3.14"') \
        .add_field(sql="CAST('a string' AS JSON)", python='"a string"') \
        .add_field(sql="CAST('a \" complex '' string :' AS JSON)", python='"a \\" complex \' string :"') \
        .add_field(sql="CAST('[]' AS JSON)", python='"[]"') \
        .execute()


def test_date(trino_connection):
    (
        SqlTest(trino_connection)
        .add_field(sql="CAST(null AS DATE)", python=None)
        .add_field(sql="DATE '0001-01-01'", python=date(1, 1, 1))  # min supported date in Python
        .add_field(sql="DATE '1582-10-04'", python=date(1582, 10, 4))  # before julian-gregorian switch
        .add_field(sql="DATE '1582-10-05'", python=date(1582, 10, 5))  # begin julian-gregorian switch
        .add_field(sql="DATE '1582-10-14'", python=date(1582, 10, 14))  # end julian-gregorian switch
        .add_field(sql="DATE '1952-04-03'", python=date(1952, 4, 3))  # before epoch
        .add_field(sql="DATE '1970-01-01'", python=date(1970, 1, 1))
        .add_field(sql="DATE '2001-08-22'", python=date(2001, 8, 22))
        .add_field(sql="DATE '9999-12-31'", python=date(9999, 12, 31))  # max supported date in Python
    ).execute()


@pytest.mark.skipif(trino_version() == 351, reason="time not rounded correctly in older Trino versions")
def test_time(trino_connection):
    (
        SqlTest(trino_connection)
        .add_field(
            sql="CAST(null AS TIME)",
            python=None)
        .add_field(
            sql="TIME '00:00:00'",
            python=time(0, 0, 0))  # min supported time(3)
        .add_field(
            sql="TIME '12:34:56.123'",
            python=time(12, 34, 56, 123000))
        .add_field(
            sql="TIME '23:59:59.999'",
            python=time(23, 59, 59, 999000))  # max supported time(3)
        # min value for each precision
        .add_field(
            sql="TIME '00:00:00'",
            python=time(0, 0, 0))
        .add_field(
            sql="TIME '00:00:00.1'",
            python=time(0, 0, 0, 100000))
        .add_field(
            sql="TIME '00:00:00.01'",
            python=time(0, 0, 0, 10000))
        .add_field(
            sql="TIME '00:00:00.001'",
            python=time(0, 0, 0, 1000))
        .add_field(
            sql="TIME '00:00:00.0001'",
            python=time(0, 0, 0, 100))
        .add_field(
            sql="TIME '00:00:00.00001'",
            python=time(0, 0, 0, 10))
        .add_field(
            sql="TIME '00:00:00.000001'",
            python=time(0, 0, 0, 1))
        # max value for each precision
        .add_field(
            sql="TIME '23:59:59'",
            python=time(23, 59, 59))
        .add_field(
            sql="TIME '23:59:59.9'",
            python=time(23, 59, 59, 900000))
        .add_field(
            sql="TIME '23:59:59.99'",
            python=time(23, 59, 59, 990000))
        .add_field(
            sql="TIME '23:59:59.999'",
            python=time(23, 59, 59, 999000))
        .add_field(
            sql="TIME '23:59:59.9999'",
            python=time(23, 59, 59, 999900))
        .add_field(
            sql="TIME '23:59:59.99999'",
            python=time(23, 59, 59, 999990))
        .add_field(
            sql="TIME '23:59:59.999999'",
            python=time(23, 59, 59, 999999))
        # round down
        .add_field(
            sql="TIME '12:34:56.1234561'",
            python=time(12, 34, 56, 123456))
        # round down, min value
        .add_field(
            sql="TIME '00:00:00.000000000001'",
            python=time(0, 0, 0, 0))
        .add_field(
            sql="TIME '00:00:00.0000001'",
            python=time(0, 0, 0, 0))
        # round down, max value
        .add_field(
            sql="TIME '00:00:00.0000004'",
            python=time(0, 0, 0, 0))
        .add_field(
            sql="TIME '00:00:00.00000049'",
            python=time(0, 0, 0, 0))
        .add_field(
            sql="TIME '00:00:00.0000005'",
            python=time(0, 0, 0, 0))
        .add_field(
            sql="TIME '00:00:00.00000050'",
            python=time(0, 0, 0, 0))
        .add_field(
            sql="TIME '23:59:59.9999994'",
            python=time(23, 59, 59, 999999))
        .add_field(
            sql="TIME '23:59:59.9999994999'",
            python=time(23, 59, 59, 999999))
        # round up
        .add_field(
            sql="TIME '12:34:56.123456789'",
            python=time(12, 34, 56, 123457))
        # round up, min value
        .add_field(
            sql="TIME '00:00:00.000000500001'",
            python=time(0, 0, 0, 1))
        # round up, max value
        .add_field(
            sql="TIME '00:00:00.0000009'",
            python=time(0, 0, 0, 1))
        .add_field(
            sql="TIME '00:00:00.00000099999'",
            python=time(0, 0, 0, 1))
        # round up to next day, min value
        .add_field(
            sql="TIME '23:59:59.9999995'",
            python=time(0, 0, 0))
        .add_field(
            sql="TIME '23:59:59.999999500001'",
            python=time(0, 0, 0))
        # round up to next day, max value
        .add_field(
            sql="TIME '23:59:59.9999999'",
            python=time(0, 0, 0))
        .add_field(
            sql="TIME '23:59:59.999999999'",
            python=time(0, 0, 0))
    ).execute()


@pytest.mark.skipif(trino_version() == 351, reason="time not rounded correctly in older Trino versions")
@pytest.mark.parametrize(
    'tz_str',
    [
        '-08:00',
        '+08:00',
        '+05:30',
    ]
)
def test_time_with_timezone(trino_connection, tz_str: str):
    tz = create_timezone(tz_str)
    (
        SqlTest(trino_connection)
        .add_field(
            sql="CAST(null AS TIME WITH TIME ZONE)",
            python=None)
        # min supported time(3)
        .add_field(
            sql=f"TIME '00:00:00 {tz_str}'",
            python=time(0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIME '12:34:56.123 {tz_str}'",
            python=time(12, 34, 56, 123000, tzinfo=tz))
        # max supported time(3)
        .add_field(
            sql=f"TIME '23:59:59.999 {tz_str}'",
            python=time(23, 59, 59, 999000, tzinfo=tz))
        # min value for each precision
        .add_field(
            sql=f"TIME '00:00:00 {tz_str}'",
            python=time(0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIME '00:00:00.1 {tz_str}'",
            python=time(0, 0, 0, 100000, tzinfo=tz))
        .add_field(
            sql=f"TIME '00:00:00.01 {tz_str}'",
            python=time(0, 0, 0, 10000, tzinfo=tz))
        .add_field(
            sql=f"TIME '00:00:00.001 {tz_str}'",
            python=time(0, 0, 0, 1000, tzinfo=tz))
        .add_field(
            sql=f"TIME '00:00:00.0001 {tz_str}'",
            python=time(0, 0, 0, 100, tzinfo=tz))
        .add_field(
            sql=f"TIME '00:00:00.00001 {tz_str}'",
            python=time(0, 0, 0, 10, tzinfo=tz))
        .add_field(
            sql=f"TIME '00:00:00.000001 {tz_str}'",
            python=time(0, 0, 0, 1, tzinfo=tz))
        # max value for each precision
        .add_field(
            sql=f"TIME '23:59:59 {tz_str}'",
            python=time(23, 59, 59, tzinfo=tz))
        .add_field(
            sql=f"TIME '23:59:59.9 {tz_str}'",
            python=time(23, 59, 59, 900000, tzinfo=tz))
        .add_field(
            sql=f"TIME '23:59:59.99 {tz_str}'",
            python=time(23, 59, 59, 990000, tzinfo=tz))
        .add_field(
            sql=f"TIME '23:59:59.999 {tz_str}'",
            python=time(23, 59, 59, 999000, tzinfo=tz))
        .add_field(
            sql=f"TIME '23:59:59.9999 {tz_str}'",
            python=time(23, 59, 59, 999900, tzinfo=tz))
        .add_field(
            sql=f"TIME '23:59:59.99999 {tz_str}'",
            python=time(23, 59, 59, 999990, tzinfo=tz))
        .add_field(
            sql=f"TIME '23:59:59.999999 {tz_str}'",
            python=time(23, 59, 59, 999999, tzinfo=tz))
        # round down
        .add_field(
            sql=f"TIME '12:34:56.1234561 {tz_str}'",
            python=time(12, 34, 56, 123456, tzinfo=tz))
        # round down, min value
        .add_field(
            sql=f"TIME '00:00:00.000000000001 {tz_str}'",
            python=time(0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIME '00:00:00.0000001 {tz_str}'",
            python=time(0, 0, 0, 0, tzinfo=tz))
        # round down, max value
        .add_field(
            sql=f"TIME '00:00:00.0000004 {tz_str}'",
            python=time(0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIME '00:00:00.00000049 {tz_str}'",
            python=time(0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIME '00:00:00.0000005 {tz_str}'",
            python=time(0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIME '23:59:59.9999994 {tz_str}'",
            python=time(23, 59, 59, 999999, tzinfo=tz))
        .add_field(
            sql=f"TIME '23:59:59.9999994999 {tz_str}'",
            python=time(23, 59, 59, 999999, tzinfo=tz))
        # round up
        .add_field(
            sql=f"TIME '12:34:56.123456789 {tz_str}'",
            python=time(12, 34, 56, 123457, tzinfo=tz))
        # round up, min value
        .add_field(
            sql=f"TIME '00:00:00.000000500001 {tz_str}'",
            python=time(0, 0, 0, 1, tzinfo=tz))
        # round up, max value
        .add_field(
            sql=f"TIME '00:00:00.0000009 {tz_str}'",
            python=time(0, 0, 0, 1, tzinfo=tz))
        .add_field(
            sql=f"TIME '00:00:00.00000099999 {tz_str}'",
            python=time(0, 0, 0, 1, tzinfo=tz))
        # round up to next day, min value
        .add_field(
            sql=f"TIME '23:59:59.9999995 {tz_str}'",
            python=time(0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIME '23:59:59.999999500001 {tz_str}'",
            python=time(0, 0, 0, tzinfo=tz))
        # round up to next day, max value
        .add_field(
            sql=f"TIME '23:59:59.9999999 {tz_str}'",
            python=time(0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIME '23:59:59.999999999 {tz_str}'",
            python=time(0, 0, 0, tzinfo=tz))
    ).execute()


def test_timestamp(trino_connection):
    (
        SqlTest(trino_connection)
        .add_field(
            sql="CAST(null AS TIMESTAMP)",
            python=None)
        # min supported timestamp(3)
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00'",
            python=datetime(2001, 8, 22, 0, 0, 0))
        .add_field(
            sql="TIMESTAMP '2001-08-22 12:34:56.123'",
            python=datetime(2001, 8, 22, 12, 34, 56, 123000))
        # max supported timestamp(3)
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.999'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999000))
        # min value for each precision
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00'",
            python=datetime(2001, 8, 22, 0, 0, 0))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.1'",
            python=datetime(2001, 8, 22, 0, 0, 0, 100000))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.01'",
            python=datetime(2001, 8, 22, 0, 0, 0, 10000))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.001'",
            python=datetime(2001, 8, 22, 0, 0, 0, 1000))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.0001'",
            python=datetime(2001, 8, 22, 0, 0, 0, 100))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.00001'",
            python=datetime(2001, 8, 22, 0, 0, 0, 10))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.000001'",
            python=datetime(2001, 8, 22, 0, 0, 0, 1))
        # max value for each precision
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59'",
            python=datetime(2001, 8, 22, 23, 59, 59))
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.9'",
            python=datetime(2001, 8, 22, 23, 59, 59, 900000))
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.99'",
            python=datetime(2001, 8, 22, 23, 59, 59, 990000))
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.999'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999000))
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.9999'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999900))
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.99999'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999990))
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.999999'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999999))
        # round down
        .add_field(
            sql="TIMESTAMP '2001-08-22 12:34:56.1234561'",
            python=datetime(2001, 8, 22, 12, 34, 56, 123456))
        # round down, min value
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.000000000001'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.0000001'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0))
        # round down, max value
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.0000004'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.00000049'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.0000005'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.00000050'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0))
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.9999994'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999999))
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.9999994999'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999999))
        # round up
        .add_field(
            sql="TIMESTAMP '2001-08-22 12:34:56.123456789'",
            python=datetime(2001, 8, 22, 12, 34, 56, 123457))
        # round up, min value
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.000000500001'",
            python=datetime(2001, 8, 22, 0, 0, 0, 1))
        # round up, max value
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.0000009'",
            python=datetime(2001, 8, 22, 0, 0, 0, 1))
        .add_field(
            sql="TIMESTAMP '2001-08-22 00:00:00.00000099999'",
            python=datetime(2001, 8, 22, 0, 0, 0, 1))
        # round up to next day, min value
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.9999995'",
            python=datetime(2001, 8, 23, 0, 0, 0, 0))
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.999999500001'",
            python=datetime(2001, 8, 23, 0, 0, 0, 0))
        # round up to next day, max value
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.9999999'",
            python=datetime(2001, 8, 23, 0, 0, 0, 0))
        .add_field(
            sql="TIMESTAMP '2001-08-22 23:59:59.999999999'",
            python=datetime(2001, 8, 23, 0, 0, 0, 0))
        # ce
        .add_field(
            sql="TIMESTAMP '0001-01-01 01:23:45.123'",
            python=datetime(1, 1, 1, 1, 23, 45, 123000))
        # julian calendar
        .add_field(
            sql="TIMESTAMP '1582-10-04 01:23:45.123'",
            python=datetime(1582, 10, 4, 1, 23, 45, 123000))
        # during switch
        .add_field(
            sql="TIMESTAMP '1582-10-05 01:23:45.123'",
            python=datetime(1582, 10, 5, 1, 23, 45, 123000))
        # gregorian calendar
        .add_field(
            sql="TIMESTAMP '1582-10-14 01:23:45.123'",
            python=datetime(1582, 10, 14, 1, 23, 45, 123000))
    ).execute()


@pytest.mark.parametrize(
    'tz_str',
    [
        '-08:00',
        '+08:00',
        'US/Eastern',
        'Asia/Kolkata',
        'GMT',
    ]
)
def test_timestamp_with_timezone(trino_connection, tz_str):
    tz = create_timezone(tz_str)
    (
        SqlTest(trino_connection)
        .add_field(
            sql="CAST(null AS TIMESTAMP WITH TIME ZONE)",
            python=None)
        # min supported timestamp(3) with time zone
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 12:34:56.123 {tz_str}'",
            python=datetime(2001, 8, 22, 12, 34, 56, 123000, tzinfo=tz))
        # max supported timestamp(3) with time zone
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.999 {tz_str}'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999000, tzinfo=tz))
        # min value for each precision
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.1 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 100000, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.01 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 10000, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.001 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 1000, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.0001 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 100, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.00001 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 10, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.000001 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 1, tzinfo=tz))
        # max value for each precision
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59 {tz_str}'",
            python=datetime(2001, 8, 22, 23, 59, 59, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.9 {tz_str}'",
            python=datetime(2001, 8, 22, 23, 59, 59, 900000, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.99 {tz_str}'",
            python=datetime(2001, 8, 22, 23, 59, 59, 990000, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.999 {tz_str}'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999000, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.9999 {tz_str}'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999900, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.99999 {tz_str}'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999990, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.999999 {tz_str}'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999999, tzinfo=tz))
        # round down
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 12:34:56.1234561 {tz_str}'",
            python=datetime(2001, 8, 22, 12, 34, 56, 123456, tzinfo=tz))
        # round down, min value
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.000000000001 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.0000001 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0, tzinfo=tz))
        # round down, max value
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.0000004 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.00000049 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.0000005 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.00000050 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.9999994 {tz_str}'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999999, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.9999994999 {tz_str}'",
            python=datetime(2001, 8, 22, 23, 59, 59, 999999, tzinfo=tz))
        # round up
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 12:34:56.123456789 {tz_str}'",
            python=datetime(2001, 8, 22, 12, 34, 56, 123457, tzinfo=tz))
        # round up, min value
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.000000500001 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 1, tzinfo=tz))
        # round up, max value
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.0000009 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 1, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 00:00:00.00000099999 {tz_str}'",
            python=datetime(2001, 8, 22, 0, 0, 0, 1, tzinfo=tz))
        # round up to next day, min value
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.9999995 {tz_str}'",
            python=datetime(2001, 8, 23, 0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.999999500001 {tz_str}'",
            python=datetime(2001, 8, 23, 0, 0, 0, 0, tzinfo=tz))
        # round up to next day, max value
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.9999999 {tz_str}'",
            python=datetime(2001, 8, 23, 0, 0, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2001-08-22 23:59:59.999999999 {tz_str}'",
            python=datetime(2001, 8, 23, 0, 0, 0, 0, tzinfo=tz))
        # ce
        .add_field(
            sql=f"TIMESTAMP '0001-01-01 01:23:45.123 {tz_str}'",
            python=datetime(1, 1, 1, 1, 23, 45, 123000, tzinfo=tz))
        # Julian calendar
        .add_field(
            sql=f"TIMESTAMP '1582-10-04 01:23:45.123 {tz_str}'",
            python=datetime(1582, 10, 4, 1, 23, 45, 123000, tzinfo=tz))
        # during switch
        .add_field(
            sql=f"TIMESTAMP '1582-10-05 01:23:45.123 {tz_str}'",
            python=datetime(1582, 10, 5, 1, 23, 45, 123000, tzinfo=tz))
        # Gregorian calendar
        .add_field(
            sql=f"TIMESTAMP '1582-10-14 01:23:45.123 {tz_str}'",
            python=datetime(1582, 10, 14, 1, 23, 45, 123000, tzinfo=tz))
    ).execute()


def test_timestamp_with_timezone_dst(trino_connection):
    tz_str = "Europe/Brussels"
    tz = create_timezone(tz_str)
    (
        SqlTest(trino_connection)
        .add_field(
            sql=f"TIMESTAMP '2022-03-27 01:59:59.999999999 {tz_str}'",
            # 2:00:00 (STD) becomes 3:00:00 (DST))
            python=datetime(2022, 3, 27, 2, 0, 0, tzinfo=tz))
        .add_field(
            sql=f"TIMESTAMP '2022-10-30 02:59:59.999999999 {tz_str}'",
            # 3:00:00 (DST) becomes 2:00:00 (STD))
            python=datetime(2022, 10, 30, 3, 0, 0, tzinfo=tz))
    ).execute()


def create_timezone(timezone_str: str) -> tzinfo:
    if timezone_str.startswith('+') or timezone_str.startswith('-'):
        # Trino doesn't support sub-hour offsets
        # trino> select timestamp '2022-01-01 01:01:01.123 UTC+05:30:10';
        # Query 20221118_120049_00002_vk3k4 failed: line 1:8: Time zone not supported: UTC+05:30:10
        hours, minutes = map(int, re.findall(r"\d{2}", timezone_str))
        if timezone_str[:1] == "+":
            return timezone(timedelta(hours=hours, minutes=minutes))
        else:
            return timezone(-timedelta(hours=hours, minutes=minutes))
    else:
        return ZoneInfo(timezone_str)


def test_interval_year_to_month(trino_connection):
    (
        SqlTest(trino_connection)
        .add_field(
            sql="CAST(null AS INTERVAL YEAR TO MONTH)",
            python=None)
        .add_field(
            sql="INTERVAL '10' YEAR",
            python=relativedelta(years=10))
        .add_field(
            sql="INTERVAL '-5' YEAR",
            python=relativedelta(years=-5))
        .add_field(
            sql="INTERVAL '3' MONTH",
            python=relativedelta(months=3))
        .add_field(
            sql="INTERVAL '-18' MONTH",
            python=relativedelta(years=-1, months=-6))
        .add_field(
            sql="INTERVAL '30' MONTH",
            python=relativedelta(years=2, months=6))
        # max supported INTERVAL in Trino
        .add_field(
            sql="INTERVAL '178956970-7' YEAR TO MONTH",
            python=relativedelta(years=178956970, months=7))
        # min supported INTERVAL in Trino
        .add_field(
            sql="INTERVAL '-178956970-8' YEAR TO MONTH",
            python=relativedelta(years=-178956970, months=-8))
    ).execute()


def test_interval_day_to_second(trino_connection):
    (
        SqlTest(trino_connection)
        .add_field(
            sql="CAST(null AS INTERVAL DAY TO SECOND)",
            python=None)
        .add_field(
            sql="INTERVAL '2' DAY",
            python=timedelta(days=2))
        .add_field(
            sql="INTERVAL '-2' DAY",
            python=timedelta(days=-2))
        .add_field(
            sql="INTERVAL '-2' SECOND",
            python=timedelta(seconds=-2))
        .add_field(
            sql="INTERVAL '1 11:11:11.116555' DAY TO SECOND",
            python=timedelta(days=1, seconds=40271, microseconds=116000))
        .add_field(
            sql="INTERVAL '-5 23:59:57.000' DAY TO SECOND",
            python=timedelta(days=-6, seconds=3))
        .add_field(
            sql="INTERVAL '12 10:45' DAY TO MINUTE",
            python=timedelta(days=12, seconds=38700))
        .add_field(
            sql="INTERVAL '45:32.123' MINUTE TO SECOND",
            python=timedelta(seconds=2732, microseconds=123000))
        .add_field(
            sql="INTERVAL '32.123' SECOND",
            python=timedelta(seconds=32, microseconds=123000))
        # max supported timedelta in Python
        .add_field(
            sql="INTERVAL '999999999 23:59:59.999' DAY TO SECOND",
            python=timedelta(days=999999999, hours=23, minutes=59, seconds=59, milliseconds=999))
        # min supported timedelta in Python
        .add_field(
            sql="INTERVAL '-999999999' DAY",
            python=timedelta(days=-999999999))
    ).execute()

    SqlExpectFailureTest(trino_connection).execute("INTERVAL '1000000000' DAY")
    SqlExpectFailureTest(trino_connection).execute("INTERVAL '-999999999 00:00:00.001' DAY TO SECOND")


def test_array(trino_connection):
    # primitive types
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS ARRAY(VARCHAR))", python=None) \
        .add_field(sql="ARRAY[]", python=[]) \
        .add_field(sql="ARRAY[true, false, null]", python=[True, False, None]) \
        .add_field(sql="ARRAY[1, 2, null]", python=[1, 2, None]) \
        .add_field(
            sql="ARRAY[CAST('NaN' AS REAL), CAST('-Infinity' AS REAL), CAST(3.4028235E38 AS REAL), CAST(1.4E-45 AS "
                "REAL), CAST('Infinity' AS REAL), null]",
            python=[math.nan, -math.inf, 3.4028235e+38, 1.4e-45, math.inf, None],
            has_nan=True) \
        .add_field(
            sql="ARRAY[CAST('NaN' AS DOUBLE), CAST('-Infinity' AS DOUBLE), CAST(1.7976931348623157E308 AS DOUBLE), "
                "CAST(4.9E-324 AS DOUBLE), CAST('Infinity' AS DOUBLE), null]",
            python=[math.nan, -math.inf, 1.7976931348623157e+308, 5e-324, math.inf, None],
            has_nan=True) \
        .add_field(sql="ARRAY[1.2, 2.4, null]", python=[Decimal("1.2"), Decimal("2.4"), None]) \
        .add_field(sql="ARRAY[CAST('hello' AS VARCHAR), null]", python=["hello", None]) \
        .add_field(sql="ARRAY[CAST('a' AS CHAR(3)), null]", python=['a  ', None]) \
        .add_field(sql="ARRAY[X'', X'65683F', null]", python=[b'', b'eh?', None]) \
        .add_field(sql="ARRAY[JSON 'null', JSON '{}', null]", python=['null', '{}', None]) \
        .execute()

    # temporal types
    SqlTest(trino_connection) \
        .add_field(sql="ARRAY[DATE '1970-01-01', null]", python=[date(1970, 1, 1), None]) \
        .add_field(sql="ARRAY[TIME '01:01:01', null]", python=[time(1, 1, 1), None]) \
        .add_field(sql="ARRAY[TIME '01:01:01 +05:30', null]",
                   python=[time(1, 1, 1, tzinfo=create_timezone("+05:30")), None]) \
        .add_field(sql="ARRAY[TIMESTAMP '1970-01-01 01:01:01', null]",
                   python=[datetime(1970, 1, 1, 1, 1, 1), None]) \
        .add_field(sql="ARRAY[TIMESTAMP '1970-01-01 01:01:01 +05:30', null]",
                   python=[datetime(1970, 1, 1, 1, 1, 1, tzinfo=create_timezone("+05:30")), None]) \
        .execute()

    # structural types
    SqlTest(trino_connection) \
        .add_field(sql="ARRAY[ARRAY[1, null], ARRAY[2, 3], null]", python=[[1, None], [2, 3], None]) \
        .add_field(
            sql="ARRAY[MAP(ARRAY['foo', 'bar', 'baz'], ARRAY['one', 'two', null]), MAP(), null]",
            python=[{"foo": "one", "bar": "two", "baz": None}, {}, None]) \
        .add_field(sql="ARRAY[ROW(1, 2), ROW(1, null), null]", python=[(1, 2), (1, None), None]) \
        .execute()


def test_map(trino_connection):
    # primitive types
    (
        SqlTest(trino_connection)
        .add_field(sql="CAST(null AS MAP(VARCHAR, INTEGER))", python=None)
        .add_field(sql="MAP()", python={})
        .add_field(sql="MAP(ARRAY[true, false], ARRAY[false, true])", python={True: False, False: True})
        .add_field(sql="MAP(ARRAY[true, false], ARRAY[true, null])", python={True: True, False: None})
        .add_field(sql="MAP(ARRAY[1, 2], ARRAY[1, null])", python={1: 1, 2: None})
        .add_field(sql="MAP("
                       "ARRAY[CAST('NaN' AS REAL), CAST('-Infinity' AS REAL), CAST(3.4028235E38 AS REAL), CAST(1.4E-45 AS REAL), CAST('Infinity' AS REAL), CAST(1 AS REAL)], "  # noqa: E501
                       "ARRAY[CAST('NaN' AS REAL), CAST('-Infinity' AS REAL), CAST(3.4028235E38 AS REAL), CAST(1.4E-45 AS REAL), CAST('Infinity' AS REAL), null])",  # noqa: E501
                   python={math.nan: math.nan,
                           -math.inf: -math.inf,
                           3.4028235e+38: 3.4028235e+38,
                           1.4e-45: 1.4e-45,
                           math.inf: math.inf,
                           1: None},
                   has_nan=True)
        .add_field(sql="MAP("
                       "ARRAY[CAST('NaN' AS DOUBLE), CAST('-Infinity' AS DOUBLE), CAST(1.7976931348623157E308 AS DOUBLE), CAST(4.9E-324 AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST(1 AS DOUBLE)], "  # noqa: E501
                       "ARRAY[CAST('NaN' AS DOUBLE), CAST('-Infinity' AS DOUBLE), CAST(1.7976931348623157E308 AS DOUBLE), CAST(4.9E-324 AS DOUBLE), CAST('Infinity' AS DOUBLE), null])",  # noqa: E501
                   python={math.nan: math.nan,
                           -math.inf: -math.inf,
                           1.7976931348623157e+308: 1.7976931348623157e+308,
                           5e-324: 5e-324,
                           math.inf: math.inf,
                           1: None},
                   has_nan=True)
        .add_field(sql="MAP(ARRAY[CAST('NaN' AS DOUBLE)], ARRAY[CAST('NaN' AS DOUBLE)])",
                   python={math.nan: math.nan},
                   has_nan=True)
        .add_field(sql="MAP(ARRAY[1.2, 2.4, 4.8], ARRAY[1.2, 2.4, null])",
                   python={Decimal("1.2"): Decimal("1.2"), Decimal("2.4"): Decimal("2.4"), Decimal("4.8"): None})
        .add_field(sql="MAP("
                       "ARRAY[CAST('hello' AS VARCHAR), CAST('null' AS VARCHAR)], "
                       "ARRAY[CAST('hello' AS VARCHAR), null])",
                   python={'hello': 'hello', 'null': None})
        .add_field(sql="MAP(ARRAY[CAST('a' AS CHAR(4)), CAST('null' AS CHAR(4))], ARRAY[CAST('a' AS CHAR), null])",
                   python={'a   ': 'a', 'null': None})
        .add_field(sql="MAP(ARRAY[X'', X'65683F', X'00'], ARRAY[X'', X'65683F', null])",
                   python={b'': b'', b'eh?': b'eh?', b'\x00': None})
        .add_field(sql="MAP(ARRAY[JSON '1', JSON '{}', JSON 'null'], ARRAY[JSON '1', JSON '{}', null])",
                   python={'1': '1', '{}': '{}', 'null': None})
    ).execute()

    # temporal types
    tz_india = create_timezone("+05:30")
    tz_new_york = create_timezone("-05:00")
    tz_los_angeles = create_timezone("America/Los_Angeles")
    time_1 = time(1, 1, 1)
    time_2 = time(23, 59, 59)
    datetime_1 = datetime(1970, 1, 1, 1, 1, 1)
    datetime_2 = datetime(2023, 1, 1, 23, 59, 59)
    SqlTest(trino_connection) \
        .add_field(sql="MAP(ARRAY[DATE '1970-01-01', DATE '2023-01-01'], ARRAY[DATE '1970-01-01', null])",
                   python={date(1970, 1, 1): date(1970, 1, 1), date(2023, 1, 1): None}) \
        .add_field(sql="MAP(ARRAY[TIME '01:01:01', TIME '23:59:59'], ARRAY[TIME '01:01:01', null])",
                   python={time_1: time_1, time_2: None}) \
        .add_field(sql="MAP("
                       "ARRAY[TIME '01:01:01 +05:30', TIME '23:59:59 -05:00'], "
                       "ARRAY[TIME '01:01:01 +05:30', null])",
                   python={time_1.replace(tzinfo=tz_india): time_1.replace(tzinfo=tz_india),
                           time_2.replace(tzinfo=tz_new_york): None}) \
        .add_field(sql="MAP("
                       "ARRAY[TIMESTAMP '1970-01-01 01:01:01', TIMESTAMP '2023-01-01 23:59:59'], "
                       "ARRAY[TIMESTAMP '1970-01-01 01:01:01', null])",
                   python={datetime_1: datetime_1, datetime_2: None}) \
        .add_field(sql="MAP("
                       "ARRAY[TIMESTAMP '1970-01-01 01:01:01 +05:30', TIMESTAMP '2023-01-01 23:59:59 America/Los_Angeles'], "  # noqa: E501
                       "ARRAY[TIMESTAMP '1970-01-01 01:01:01 +05:30', null])",
                   python={datetime_1.replace(tzinfo=tz_india): datetime_1.replace(tzinfo=tz_india),
                           datetime_2.replace(tzinfo=tz_los_angeles): None}) \
        .execute()

    # structural types - note that none of these below tests work in the Trino JDBC Driver either.
    # TODO: https://github.com/trinodb/trino-python-client/issues/442
    # Unhashable types like lists and dicts cannot be used as keys so these values cannot be represented as Python
    #  objects at all.
    # .add_field(sql="MAP(ARRAY[ARRAY[1, 2]], ARRAY[null])", python={[1, 2]: None})
    # .add_field(sql="MAP(ARRAY[MAP(ARRAY[1], ARRAY[2])], ARRAY[null])", python={{1: 2}: None})

    # TODO: fails because server sends [[{"[1, 2]":null}]] as response whereas it sends [[[1,2]]] as response for ROW
    #  types that are not enclosed in a MAP while the RowValueMapper expects values to be lists.
    # .add_field(sql="MAP(ARRAY[ROW(1, 2)], ARRAY[CAST(null AS VARCHAR)])", python={(1, 2): None})


def test_row(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS ROW(x BIGINT, y DOUBLE))", python=None) \
        .add_field(sql="CAST(ROW(1, 2e0, null) AS ROW(x BIGINT, y DOUBLE, z DOUBLE))", python=(1, 2.0, None)) \
        .execute()


def test_ipaddress(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS IPADDRESS)", python=None) \
        .add_field(sql="IPADDRESS '2001:db8::1'", python='2001:db8::1') \
        .execute()


def test_uuid(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS UUID)", python=None) \
        .add_field(sql="UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'",
                   python=uuid.UUID('12151fd2-7586-11e9-8f9e-2a86e4085a59')) \
        .execute()


def test_digest(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS HyperLogLog)", python=None) \
        .add_field(sql="CAST(null AS P4HyperLogLog)", python=None) \
        .add_field(sql="CAST(null AS SetDigest)", python=None) \
        .add_field(sql="CAST(null AS QDigest(BIGINT))", python=None) \
        .add_field(sql="CAST(null AS TDigest)", python=None) \
        .add_field(sql="approx_set(1)", python='AgwBAIADRAA=') \
        .add_field(sql="CAST(approx_set(1) AS P4HyperLogLog)", python='AwwAAAAg' + 'A' * 2730 + '==') \
        .add_field(sql="make_set_digest(1)", python='AQgAAAACCwEAgANEAAAgAAABAAAASsQF+7cDRAABAA==') \
        .add_field(sql="tdigest_agg(1)",
                   python='AAAAAAAAAPA/AAAAAAAA8D8AAAAAAABZQAAAAAAAAPA/AQAAAAAAAAAAAPA/AAAAAAAA8D8=') \
        .execute()


class SqlTest:
    def __init__(self, trino_connection):
        self.cur = trino_connection.cursor(legacy_primitive_types=False)
        self.sql_args = []
        self.expected_results = []
        self.has_nan = []

    def add_field(self, sql, python, has_nan=False):
        self.sql_args.append(sql)
        self.expected_results.append(python)
        self.has_nan.append(has_nan)
        return self

    def execute(self):
        sql = 'SELECT ' + ',\n'.join(self.sql_args)

        self.cur.execute(sql)
        actual_results = self.cur.fetchall()
        self._compare_results(actual_results[0], self.expected_results)

    def _are_equal_ignoring_nan(self, actual, expected) -> bool:
        if isinstance(actual, float) and math.isnan(actual) \
                and isinstance(expected, float) and math.isnan(expected):
            # Consider NaNs equal since we only want to make sure values round-trip
            return True
        return actual == expected

    def _compare_results(self, actual_results, expected_results):
        assert len(actual_results) == len(expected_results)

        for idx, actual in enumerate(actual_results):
            expected = expected_results[idx]
            if not self.has_nan[idx]:
                assert actual == expected
            else:
                # We need to consider NaNs in a collection equal since we only want to make sure values round-trip.
                # collections compare identity first instead of value so:
                # >>> from math import nan
                # >>> [nan] == [nan]
                # True
                # >>> [nan] == [float("nan")]
                # False
                # >>> [float("nan")] == [float("nan")]
                # False
                # We create the NaNs using float("nan") which means PyTest's assert
                # will always fail on collections containing nan.
                if (isinstance(actual, list) and isinstance(expected, list)) \
                        or (isinstance(actual, set) and isinstance(expected, set)) \
                        or (isinstance(actual, tuple) and isinstance(expected, tuple)):
                    for i, _ in enumerate(actual):
                        if not self._are_equal_ignoring_nan(actual[i], expected[i]):
                            # Will fail, here to provide useful assertion message
                            assert actual == expected
                elif isinstance(actual, dict) and isinstance(expected, dict):
                    for actual_key, actual_value in actual.items():
                        # Note that Trino disallows multiple NaN keys in a MAP, so we don't consider the case where
                        # multiple NaN keys exist in either dict.
                        if math.isnan(actual_key):
                            expected_has_nan_key = False
                            for expected_key, expected_value in expected.items():
                                if math.isnan(expected_key):
                                    expected_has_nan_key = True
                                    # Found the other NaN key. Let's compare the values from both dicts.
                                    if not self._are_equal_ignoring_nan(actual_value, expected_value):
                                        # Will fail, here to provide useful assertion message
                                        assert actual == expected
                            # If expected has no NaN keys then the dicts cannot be equal since actual has a NaN key.
                            if not expected_has_nan_key:
                                # Will fail, here to provide useful assertion message
                                assert actual == expected
                        else:
                            if not self._are_equal_ignoring_nan(actual.get(actual_key), expected.get(actual_key)):
                                # Will fail, here to provide useful assertion message
                                assert actual == expected
                else:
                    if not self._are_equal_ignoring_nan(actual, expected):
                        # Will fail, here to provide useful assertion message
                        assert actual == expected


class SqlExpectFailureTest:
    def __init__(self, trino_connection):
        self.cur = trino_connection.cursor(legacy_primitive_types=False)

    def execute(self, field):
        sql = 'SELECT ' + field

        try:
            self.cur.execute(sql)
            self.cur.fetchall()
            success = True
        except Exception:
            success = False

        assert not success, "Test not expected to succeed"
