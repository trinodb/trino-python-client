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
from sqlalchemy.sql.sqltypes import (
    CHAR,
    VARCHAR,
    ARRAY,
    INTEGER,
    DECIMAL,
    DATE
)
from sqlalchemy.sql.type_api import TypeEngine

from trino.sqlalchemy import datatype
from trino.sqlalchemy.datatype import (
    MAP,
    ROW,
    TIME,
    TIMESTAMP
)


@pytest.mark.parametrize(
    "type_str, sql_type",
    datatype._type_map.items(),
    ids=datatype._type_map.keys(),
)
def test_parse_simple_type(type_str: str, sql_type: TypeEngine, assert_sqltype):
    actual_type = datatype.parse_sqltype(type_str)
    if not isinstance(actual_type, type):
        actual_type = type(actual_type)
    assert_sqltype(actual_type, sql_type)


parse_cases_testcases = {
    "char(10)": CHAR(10),
    "Char(10)": CHAR(10),
    "char": CHAR(),
    "cHaR": CHAR(),
    "VARCHAR(10)": VARCHAR(10),
    "varCHAR(10)": VARCHAR(10),
    "VARchar(10)": VARCHAR(10),
    "VARCHAR": VARCHAR(),
    "VaRchAr": VARCHAR(),
}


@pytest.mark.parametrize(
    "type_str, sql_type",
    parse_cases_testcases.items(),
    ids=parse_cases_testcases.keys(),
)
def test_parse_cases(type_str: str, sql_type: TypeEngine, assert_sqltype):
    actual_type = datatype.parse_sqltype(type_str)
    assert_sqltype(actual_type, sql_type)


parse_type_options_testcases = {
    "CHAR(10)": CHAR(10),
    "VARCHAR(10)": VARCHAR(10),
    "DECIMAL(20)": DECIMAL(20),
    "DECIMAL(20, 3)": DECIMAL(20, 3)
}


@pytest.mark.parametrize(
    "type_str, sql_type",
    parse_type_options_testcases.items(),
    ids=parse_type_options_testcases.keys(),
)
def test_parse_type_options(type_str: str, sql_type: TypeEngine, assert_sqltype):
    actual_type = datatype.parse_sqltype(type_str)
    assert_sqltype(actual_type, sql_type)


parse_array_testcases = {
    "array(integer)": ARRAY(INTEGER()),
    "array(varchar(10))": ARRAY(VARCHAR(10)),
    "array(decimal(20,3))": ARRAY(DECIMAL(20, 3)),
    "array(array(varchar(10)))": ARRAY(VARCHAR(10), dimensions=2),
    "array(map(char, integer))": ARRAY(MAP(CHAR(), INTEGER())),
    "array(row(a integer, b varchar))": ARRAY(ROW([("a", INTEGER()), ("b", VARCHAR())])),
}


@pytest.mark.parametrize(
    "type_str, sql_type",
    parse_array_testcases.items(),
    ids=parse_array_testcases.keys(),
)
def test_parse_array(type_str: str, sql_type: ARRAY, assert_sqltype):
    actual_type = datatype.parse_sqltype(type_str)
    assert_sqltype(actual_type, sql_type)


parse_map_testcases = {
    "map(char, integer)": MAP(CHAR(), INTEGER()),
    "map(varchar(10), varchar(10))": MAP(VARCHAR(10), VARCHAR(10)),
    "map(varchar(10), decimal(20,3))": MAP(VARCHAR(10), DECIMAL(20, 3)),
    "map(char, array(varchar(10)))": MAP(CHAR(), ARRAY(VARCHAR(10))),
    "map(varchar(10), array(varchar(10)))": MAP(VARCHAR(10), ARRAY(VARCHAR(10))),
    "map(varchar(10), array(array(varchar(10))))": MAP(VARCHAR(10), ARRAY(VARCHAR(10), dimensions=2)),
}


@pytest.mark.parametrize(
    "type_str, sql_type",
    parse_map_testcases.items(),
    ids=parse_map_testcases.keys(),
)
def test_parse_map(type_str: str, sql_type: ARRAY, assert_sqltype):
    actual_type = datatype.parse_sqltype(type_str)
    assert_sqltype(actual_type, sql_type)


parse_row_testcases = {
    "row(a integer, b varchar)": ROW(
        attr_types=[
            ("a", INTEGER()),
            ("b", VARCHAR()),
        ]
    ),
    "row(a varchar(20), b decimal(20,3))": ROW(
        attr_types=[
            ("a", VARCHAR(20)),
            ("b", DECIMAL(20, 3)),
        ]
    ),
    "row(x array(varchar(10)), y array(array(varchar(10))), z decimal(20,3))": ROW(
        attr_types=[
            ("x", ARRAY(VARCHAR(10))),
            ("y", ARRAY(VARCHAR(10), dimensions=2)),
            ("z", DECIMAL(20, 3)),
        ]
    ),
    "row(min timestamp(6) with time zone, max timestamp(6) with time zone)": ROW(
        attr_types=[
            ("min", TIMESTAMP(6, timezone=True)),
            ("max", TIMESTAMP(6, timezone=True)),
        ]
    ),
    'row("first name" varchar, "last name" varchar)': ROW(
        attr_types=[
            ("first name", VARCHAR()),
            ("last name", VARCHAR()),
        ]
    ),
    'row("foo,bar" varchar, "foo(bar)" varchar, "foo\\"bar" varchar)': ROW(
        attr_types=[
            (r"foo,bar", VARCHAR()),
            (r"foo(bar)", VARCHAR()),
            (r'foo"bar', VARCHAR()),
        ]
    ),
}


@pytest.mark.parametrize(
    "type_str, sql_type",
    parse_row_testcases.items(),
    ids=parse_row_testcases.keys(),
)
def test_parse_row(type_str: str, sql_type: ARRAY, assert_sqltype):
    actual_type = datatype.parse_sqltype(type_str)
    assert_sqltype(actual_type, sql_type)


parse_datetime_testcases = {
    "date": DATE(),
    "time": TIME(),
    "time(0)": TIME(0),
    "time(3)": TIME(3, timezone=False),
    "time(6)": TIME(6),
    "time(13)": TIME(13),
    "time(12) with time zone": TIME(12, timezone=True),
    "time with time zone": TIME(timezone=True),
    "timestamp(0)": TIMESTAMP(0),
    "timestamp(3)": TIMESTAMP(3, timezone=False),
    "timestamp(6)": TIMESTAMP(6),
    "timestamp(12) with time zone": TIMESTAMP(12, timezone=True),
    "timestamp with time zone": TIMESTAMP(timezone=True)
}


@pytest.mark.parametrize(
    "type_str, sql_type",
    parse_datetime_testcases.items(),
    ids=parse_datetime_testcases.keys(),
)
def test_parse_datetime(type_str: str, sql_type: TypeEngine, assert_sqltype):
    actual_type = datatype.parse_sqltype(type_str)
    assert_sqltype(actual_type, sql_type)
