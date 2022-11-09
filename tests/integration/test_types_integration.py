import math
from decimal import Decimal

import pytest

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
        .add_field(sql="ARRAY[1.2, 2.4, null]", python=[Decimal("1.2"), Decimal("2.4"), None]) \
        .add_field(sql="ARRAY[CAST(4.9E-324 AS DOUBLE), null]", python=[5e-324, None]) \
        .execute()


def test_map(trino_connection):
    SqlTest(trino_connection) \
        .add_field(sql="CAST(null AS MAP(VARCHAR, INTEGER))", python=None) \
        .add_field(sql="MAP(ARRAY['a', 'b'], ARRAY[1, null])", python={'a': 1, 'b': None}) \
        .add_field(sql="MAP(ARRAY['a', 'b'], ARRAY[2.4, null])", python={'a': Decimal("2.4"), 'b': None}) \
        .add_field(sql="MAP(ARRAY[2.4, 4.8], ARRAY[CAST(4.9E-324 AS DOUBLE), null])",
                   python={Decimal("2.4"): 5e-324, Decimal("4.8"): None}) \
        .execute()


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
