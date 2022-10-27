import math
from datetime import timedelta, datetime, date, time
import pytest
import pytz
from decimal import Decimal
import trino


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
        .add_field(sql="CAST('NaN' AS REAL)", python=math.nan) \
        .add_field(sql="CAST('-Infinity' AS REAL)", python=-math.inf) \
        .add_field(sql="CAST(3.4028235E38 AS REAL)", python=3.4028235e+38) \
        .add_field(sql="CAST(1.4E-45 AS REAL)", python=1.4e-45) \
        .add_field(sql="CAST('Infinity' AS REAL)", python=math.inf) \
        .execute()


def test_double(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS DOUBLE)", python=None) \
        .add_field(sql="CAST('NaN' AS DOUBLE)", python=math.nan) \
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
        .add_field(sql="CAST(null AS CHAR)", python=None) \
        .add_field(sql="CAST('ddd' AS CHAR(1))", python='d') \
        .add_field(sql="CAST('😂' AS CHAR(1))", python='😂') \
        .add_field(sql="CAST(null AS CHAR(1))", python=None) \
        .execute()


def test_varbinary(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="X'65683F'", python='ZWg/') \
        .add_field(sql="X''", python='') \
        .add_field(sql="CAST('' AS VARBINARY)", python='') \
        .add_field(sql="from_utf8(CAST('😂😂😂😂😂😂' AS VARBINARY))", python='😂😂😂😂😂😂') \
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


def test_interval(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS INTERVAL YEAR TO MONTH)", python=None) \
        .add_field(sql="CAST(null AS INTERVAL DAY TO SECOND)", python=None) \
        .add_field(sql="INTERVAL '3' MONTH", python='0-3') \
        .add_field(sql="INTERVAL '2' DAY", python='2 00:00:00.000') \
        .add_field(sql="INTERVAL '-2' DAY", python='-2 00:00:00.000') \
        .execute()


def test_array(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS ARRAY(VARCHAR))", python=None) \
        .add_field(sql="ARRAY['a', 'b', null]", python=['a', 'b', None]) \
        .execute()


def test_map(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS MAP(VARCHAR, INTEGER))", python=None) \
        .add_field(sql="MAP(ARRAY['a', 'b'], ARRAY[1, null])", python={'a': 1, 'b': None}) \
        .execute()


def test_row(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS ROW(x BIGINT, y DOUBLE))", python=None) \
        .add_field(sql="CAST(ROW(1, 2e0) AS ROW(x BIGINT, y DOUBLE))", python=(1, 2.0)) \
        .execute()


def test_ipaddress(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS IPADDRESS)", python=None) \
        .add_field(sql="IPADDRESS '2001:db8::1'", python='2001:db8::1') \
        .execute()


def test_uuid(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS UUID)", python=None) \
        .add_field(sql="UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'", python='12151fd2-7586-11e9-8f9e-2a86e4085a59') \
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


def test_date(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS DATE)", python=None) \
        .add_field(sql="DATE '2001-08-22'", python=date(2001, 8, 22)) \
        .add_field(sql="DATE '0001-01-01'", python=date(1, 1, 1)) \
        .add_field(sql="DATE '1582-10-04'", python=date(1582, 10, 4)) \
        .add_field(sql="DATE '1582-10-05'", python=date(1582, 10, 5)) \
        .add_field(sql="DATE '1582-10-14'", python=date(1582, 10, 14)) \
        .execute()


def test_time(trino_connection):
    time_0 = time(1, 23, 45)
    time_3 = time(1, 23, 45, 123000)
    time_6 = time(1, 23, 45, 123456)
    time_round = time(1, 23, 45, 123457)

    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS TIME)", python=None) \
        .add_field(sql="CAST(null AS TIME(0))", python=None) \
        .add_field(sql="CAST(null AS TIME(3))", python=None) \
        .add_field(sql="CAST(null AS TIME(6))", python=None) \
        .add_field(sql="CAST(null AS TIME(9))", python=None) \
        .add_field(sql="CAST(null AS TIME(12))", python=None) \
        .add_field(sql="CAST('01:23:45' AS TIME(0))", python=time_0) \
        .add_field(sql="TIME '01:23:45.123'", python=time_3) \
        .add_field(sql="CAST('01:23:45.123' AS TIME(3))", python=time_3) \
        .add_field(sql="CAST('01:23:45.123456' AS TIME(6))", python=time_6) \
        .add_field(sql="CAST('01:23:45.123456789' AS TIME(9))", python=time_round) \
        .add_field(sql="CAST('01:23:45.123456789123' AS TIME(12))", python=time_round) \
        .execute()


def test_time_with_timezone(trino_connection):
    query_time_with_timezone(trino_connection, '-08:00')
    query_time_with_timezone(trino_connection, '+08:00')
    query_time_with_timezone(trino_connection, '+05:30')


def query_time_with_timezone(trino_connection, tz_str):
    tz = datetime.strptime('+00:00', "%z").tzinfo

    hours_shift = int(tz_str[:3])
    minutes_shift = int(tz_str[4:])
    delta = timedelta(hours=hours_shift, minutes=minutes_shift)

    time_0 = (datetime(2, 1, 1, 11, 23, 45, 0) - delta).time().replace(tzinfo=tz)
    time_3 = (datetime(2, 1, 1, 11, 23, 45, 123000) - delta).time().replace(tzinfo=tz)
    time_6 = (datetime(2, 1, 1, 11, 23, 45, 123456) - delta).time().replace(tzinfo=tz)
    time_round = (datetime(2, 1, 1, 11, 23, 45, 123457) - delta).time().replace(tzinfo=tz)

    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS TIME WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST(null AS TIME(0) WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST(null AS TIME(3) WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST(null AS TIME(6) WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST(null AS TIME(9) WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST(null AS TIME(12) WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST('11:23:45 %s' AS TIME(0) WITH TIME ZONE)" % (tz_str), python=time_0) \
        .add_field(sql="TIME '11:23:45.123 %s'" % (tz_str), python=time_3) \
        .add_field(sql="CAST('11:23:45.123 %s' AS TIME(3) WITH TIME ZONE)" % (tz_str), python=time_3) \
        .add_field(sql="CAST('11:23:45.123456 %s' AS TIME(6) WITH TIME ZONE)" % (tz_str), python=time_6) \
        .add_field(sql="CAST('11:23:45.123456789 %s' AS TIME(9) WITH TIME ZONE)" % (tz_str), python=time_round) \
        .add_field(sql="CAST('11:23:45.123456789123 %s' AS TIME(12) WITH TIME ZONE)" % (tz_str), python=time_round) \
        .execute()


def test_timestamp(trino_connection):
    timestamp_0 = datetime(2001, 8, 22, 1, 23, 45, 0)
    timestamp_3 = datetime(2001, 8, 22, 1, 23, 45, 123000)
    timestamp_6 = datetime(2001, 8, 22, 1, 23, 45, 123456)
    timestamp_round = datetime(2001, 8, 22, 1, 23, 45, 123457)
    timestamp_ce = datetime(1, 1, 1, 1, 23, 45, 123000)
    timestamp_julian = datetime(1582, 10, 4, 1, 23, 45, 123000)
    timestamp_during_switch = datetime(1582, 10, 5, 1, 23, 45, 123000)
    timestamp_gregorian = datetime(1582, 10, 14, 1, 23, 45, 123000)

    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS TIMESTAMP)", python=None) \
        .add_field(sql="CAST(null AS TIMESTAMP(0))", python=None) \
        .add_field(sql="CAST(null AS TIMESTAMP(3))", python=None) \
        .add_field(sql="CAST(null AS TIMESTAMP(6))", python=None) \
        .add_field(sql="CAST(null AS TIMESTAMP(9))", python=None) \
        .add_field(sql="CAST(null AS TIMESTAMP(12))", python=None) \
        .add_field(sql="CAST('2001-08-22 01:23:45' AS TIMESTAMP(0))", python=timestamp_0) \
        .add_field(sql="TIMESTAMP '2001-08-22 01:23:45.123'", python=timestamp_3) \
        .add_field(sql="TIMESTAMP '0001-01-01 01:23:45.123'", python=timestamp_ce) \
        .add_field(sql="TIMESTAMP '1582-10-04 01:23:45.123'", python=timestamp_julian) \
        .add_field(sql="TIMESTAMP '1582-10-05 01:23:45.123'", python=timestamp_during_switch) \
        .add_field(sql="TIMESTAMP '1582-10-14 01:23:45.123'", python=timestamp_gregorian) \
        .add_field(sql="CAST('2001-08-22 01:23:45.123' AS TIMESTAMP(3))", python=timestamp_3) \
        .add_field(sql="CAST('2001-08-22 01:23:45.123456' AS TIMESTAMP(6))", python=timestamp_6) \
        .add_field(sql="CAST('2001-08-22 01:23:45.123456111' AS TIMESTAMP(9))", python=timestamp_6) \
        .add_field(sql="CAST('2001-08-22 01:23:45.123456789' AS TIMESTAMP(9))", python=timestamp_round) \
        .add_field(sql="CAST('2001-08-22 01:23:45.123456111111' AS TIMESTAMP(12))", python=timestamp_6) \
        .add_field(sql="CAST('2001-08-22 01:23:45.123456789123' AS TIMESTAMP(12))", python=timestamp_round) \
        .execute()


def test_timestamp_with_timezone(trino_connection):
    query_timestamp_with_timezone(trino_connection, '-08:00')
    query_timestamp_with_timezone(trino_connection, '+08:00')
    query_timestamp_with_timezone(trino_connection, '+05:30')
    query_timestamp_with_timezone(trino_connection, 'US/Eastern')
    query_timestamp_with_timezone(trino_connection, 'Asia/Kolkata')
    query_timestamp_with_timezone(trino_connection, 'GMT')


def query_timestamp_with_timezone(trino_connection, tz_str):
    if tz_str.startswith('+') or tz_str.startswith('-'):
        hours_shift = int(tz_str[:3])
        minutes_shift = int(tz_str[4:])
    else:
        tz = pytz.timezone(tz_str)
        offset = tz.utcoffset(datetime.now())
        offset_seconds = offset.total_seconds()
        hours_shift = int(offset_seconds / 3600)
        minutes_shift = offset_seconds % 3600 / 60

    tz = pytz.timezone('Etc/GMT')
    delta = timedelta(hours=hours_shift, minutes=minutes_shift)

    timestamp_0 = tz.localize(datetime(2001, 8, 22, 11, 23, 45, 0)) - delta
    timestamp_3 = tz.localize(datetime(2001, 8, 22, 11, 23, 45, 123000)) - delta
    timestamp_6 = tz.localize(datetime(2001, 8, 22, 11, 23, 45, 123456)) - delta
    timestamp_round = tz.localize(datetime(2001, 8, 22, 11, 23, 45, 123457)) - delta

    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS TIMESTAMP WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST(null AS TIMESTAMP(0) WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST(null AS TIMESTAMP(3) WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST(null AS TIMESTAMP(6) WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST(null AS TIMESTAMP(9) WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST(null AS TIMESTAMP(12) WITH TIME ZONE)", python=None) \
        .add_field(sql="CAST('2001-08-22 11:23:45 %s' AS TIMESTAMP(0) WITH TIME ZONE)" % (tz_str),
                   python=timestamp_0) \
        .add_field(sql="TIMESTAMP '2001-08-22 11:23:45.123 %s'" % (tz_str),
                   python=timestamp_3) \
        .add_field(sql="CAST('2001-08-22 11:23:45.123 %s' AS TIMESTAMP(3) WITH TIME ZONE)" % (tz_str),
                   python=timestamp_3) \
        .add_field(sql="CAST('2001-08-22 11:23:45.123456 %s' AS TIMESTAMP(6) WITH TIME ZONE)" % (tz_str),
                   python=timestamp_6) \
        .add_field(sql="CAST('2001-08-22 11:23:45.123456111 %s' AS TIMESTAMP(9) WITH TIME ZONE)" % (tz_str),
                   python=timestamp_6) \
        .add_field(sql="CAST('2001-08-22 11:23:45.123456789 %s' AS TIMESTAMP(9) WITH TIME ZONE)" % (tz_str),
                   python=timestamp_round) \
        .add_field(sql="CAST('2001-08-22 11:23:45.123456111111 %s' AS TIMESTAMP(12) WITH TIME ZONE)" % (tz_str),
                   python=timestamp_6) \
        .add_field(sql="CAST('2001-08-22 11:23:45.123456789123 %s' AS TIMESTAMP(12) WITH TIME ZONE)" % (tz_str),
                   python=timestamp_round) \
        .execute()


class SqlTest:
    def __init__(self, trino_connection):
        self.cur = trino_connection.cursor(experimental_python_types=True)
        self.sql_args = []
        self.expected_result = []

    def add_field(self, sql, python):
        self.sql_args.append(sql)
        self.expected_result.append(python)
        return self

    def execute(self):
        sql = 'SELECT ' + ',\n'.join(self.sql_args)

        self.cur.execute(sql)
        actual_result = self.cur.fetchall()
        self._compare_results(actual_result[0], self.expected_result)

    def _compare_results(self, actual, expected):
        assert len(actual) == len(expected)

        for idx, actual_val in enumerate(actual):
            expected_val = expected[idx]
            if type(actual_val) == float and math.isnan(actual_val) \
               and type(expected_val) == float and math.isnan(expected_val):
                continue

            assert actual_val == expected_val


class SqlExpectFailureTest:
    def __init__(self, trino_connection):
        self.cur = trino_connection.cursor(experimental_python_types=True)

    def execute(self, field):
        sql = 'SELECT ' + field

        try:
            self.cur.execute(sql)
            self.cur.fetchall()
            success = True
        except Exception:
            success = False

        assert not success, "Test not expected to succeed"
