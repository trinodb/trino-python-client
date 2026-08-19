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
import datetime
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.exc import CompileError
from sqlalchemy.exc import SAWarning
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import column
from sqlalchemy.sql import table

from tests.unit.conftest import sqlalchemy_version
from trino.sqlalchemy.datatype import DATE
from trino.sqlalchemy.datatype import TIME
from trino.sqlalchemy.datatype import TIMESTAMP
from trino.sqlalchemy.dialect import TrinoDialect

metadata = MetaData()
table_without_catalog = Table(
    'table',
    metadata,
    Column('id', Integer),
    Column('name', String),
)
table_with_catalog = Table(
    'table',
    metadata,
    Column('id', Integer),
    schema='default',
    trino_catalog='other'
)

table_with_pk = Table(
    'table_with_pk',
    metadata,
    Column('id', String, primary_key=True)
)

table_with_fk = Table(
    'table_with_fk',
    metadata,
    Column('id', String, primary_key=True),
    Column('fk', String, ForeignKey('table_with_pk.id'))
)

table_with_unique = Table(
    'table_with_constraint',
    metadata,
    Column('id', String, primary_key=True),
    Column('uniq', String, unique=True)
)


@pytest.fixture
def dialect():
    return TrinoDialect()


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
def test_limit_offset(dialect):
    statement = select(table_without_catalog).limit(10).offset(0)
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nOFFSET :param_1\nLIMIT :param_2'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
def test_limit(dialect):
    statement = select(table_without_catalog).limit(10)
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nLIMIT :param_1'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
def test_offset(dialect):
    statement = select(table_without_catalog).offset(0)
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nOFFSET :param_1'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
def test_cte_insert_order(dialect):
    cte = select(table_without_catalog).cte('cte')
    statement = insert(table_without_catalog).from_select(table_without_catalog.columns, cte)
    query = statement.compile(dialect=dialect)
    assert str(query) == \
        'INSERT INTO "table" (id, name) WITH cte AS \n'\
        '(SELECT "table".id AS id, "table".name AS name \n'\
        'FROM "table")\n'\
        ' SELECT cte.id, cte.name \n'\
        'FROM cte'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
def test_catalogs_argument(dialect):
    statement = select(table_with_catalog)
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT default."table".id \nFROM "other".default."table"'


def test_catalogs_create_table(dialect):
    statement = CreateTable(table_with_catalog)
    query = statement.compile(dialect=dialect)
    assert str(query) == \
        '\n'\
        'CREATE TABLE "other".default."table" (\n'\
        '\tid INTEGER\n'\
        ')\n'\
        '\n'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
def test_table_clause(dialect):
    statement = select(table("user", column("id"), column("name"), column("description")))
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT user.id, user.name, user.description \nFROM user'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize(
    'function,element',
    [
        ('first_value', func.first_value),
        ('last_value', func.last_value),
        ('nth_value', func.nth_value),
        ('lead', func.lead),
        ('lag', func.lag),
    ]
)
def test_ignore_nulls(dialect, function, element):
    statement = select(
        element(
            table_without_catalog.c.id,
            ignore_nulls=True,
        ).over(partition_by=table_without_catalog.c.name).label('window')
    )
    query = statement.compile(dialect=dialect)
    assert str(query) == \
           f'SELECT {function}("table".id) IGNORE NULLS OVER (PARTITION BY "table".name) AS window '\
           f'\nFROM "table"'

    statement = select(
        element(
            table_without_catalog.c.id,
            ignore_nulls=False,
        ).over(partition_by=table_without_catalog.c.name).label('window')
    )
    query = statement.compile(dialect=dialect)
    assert str(query) == \
           f'SELECT {function}("table".id) OVER (PARTITION BY "table".name) AS window ' \
           f'\nFROM "table"'

    # testing with compile kwargs
    statement = select(
        element(
            func.round(table_without_catalog.c.id, 2),
            ignore_nulls=True,
        ).over(partition_by=table_without_catalog.c.name).label('window')
    )
    query = statement.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    assert str(query) == \
           f'SELECT {function}(round("table".id, 2)) IGNORE NULLS OVER (PARTITION BY "table".name) AS window '\
           f'\nFROM "table"'


@pytest.mark.skipif(
    sqlalchemy_version() < "2.0",
    reason="ImportError: cannot import name 'try_cast' from 'sqlalchemy'"
)
def test_try_cast(dialect):
    from sqlalchemy import try_cast
    statement = select(try_cast(table_without_catalog.c.id, String))
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT try_cast("table".id as VARCHAR) AS id \nFROM "table"'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize(
    'col_type,value,expected',
    [
        (TIMESTAMP(),
         datetime.datetime(2026, 6, 17, 9, 57, 43, 244000),
         "TIMESTAMP '2026-06-17 09:57:43.244000'"),
        (TIMESTAMP(),
         datetime.datetime(2026, 6, 17, 9, 57, 43),
         "TIMESTAMP '2026-06-17 09:57:43.000000'"),
        (TIMESTAMP(),
         datetime.datetime(5, 6, 17, 9, 57, 43),
         "TIMESTAMP '0005-06-17 09:57:43.000000'"),
        (TIMESTAMP(timezone=True),
         datetime.datetime(2026, 6, 17, 9, 57, 43, 244000,
                           tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30))),
         "TIMESTAMP '2026-06-17 09:57:43.244000 +05:30'"),
        (TIMESTAMP(timezone=True),
         datetime.datetime(2026, 6, 17, 9, 57, 43, 244000, tzinfo=ZoneInfo("Asia/Kolkata")),
         "TIMESTAMP '2026-06-17 09:57:43.244000 Asia/Kolkata'"),
        (TIME(),
         datetime.time(9, 57, 43, 244000),
         "TIME '09:57:43.244000'"),
        (TIME(),
         datetime.time(9, 57, 43),
         "TIME '09:57:43.000000'"),
        (TIME(timezone=True),
         datetime.time(9, 57, 43, 244000, tzinfo=datetime.timezone(datetime.timedelta(hours=-8))),
         "TIME '09:57:43.244000 -08:00'"),
        # Asia/Kolkata has kept the same offset since 1945, so resolving the
        # zone against the current date stays deterministic.
        (TIME(timezone=True),
         datetime.time(9, 57, 43, 244000, tzinfo=ZoneInfo("Asia/Kolkata")),
         "TIME '09:57:43.244000 +05:30'"),
        (DATE(),
         datetime.date(2026, 6, 17),
         "DATE '2026-06-17'"),
        (DATE(),
         datetime.date(5, 6, 17),
         "DATE '0005-06-17'"),
    ]
)
def test_temporal_literal_processor(dialect, col_type, value, expected):
    col = column("col", col_type)
    tbl = table("t", col)
    stmt = select(tbl).where(col == value)
    query = stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    assert expected in str(query)


@pytest.mark.parametrize(
    'col_type,value',
    [
        (TIMESTAMP(), "not-a-timestamp"),
        (TIMESTAMP(), datetime.date(2026, 6, 17)),
        (TIME(), "not-a-time"),
        (DATE(), "not-a-date"),
        (DATE(), datetime.datetime(2026, 6, 17, 9, 57, 43)),
    ]
)
def test_temporal_literal_processor_invalid_value(dialect, col_type, value):
    process = col_type.literal_processor(dialect)
    with pytest.raises(CompileError):
        process(value)


class _FakeDstZone(datetime.tzinfo):
    # Behaves like a named zone: no offset without a date. The offset for a
    # date is mutable so a test can simulate crossing a DST transition.
    def __init__(self):
        self.current_offset = datetime.timedelta(hours=-5)

    def utcoffset(self, dt):
        return None if dt is None else self.current_offset

    def dst(self, dt):
        return None if dt is None else datetime.timedelta(0)

    def tzname(self, dt):
        return "Fake/Eastern"


def test_time_literal_processor_named_zone_offset_varies_with_date(dialect):
    # The renderer resolves a named zone against the current date, so the
    # same time value renders different literals on different days.
    # trino.temporal carries a TODO to reject named-zone times instead.
    process = TIME().literal_processor(dialect)
    tz = _FakeDstZone()
    value = datetime.time(9, 57, 43, tzinfo=tz)
    assert process(value) == "TIME '09:57:43.000000 -05:00'"
    tz.current_offset = datetime.timedelta(hours=-4)
    assert process(value) == "TIME '09:57:43.000000 -04:00'"


def test_timestamp_literal_processor_sub_minute_offset(dialect):
    process = TIMESTAMP().literal_processor(dialect)
    tz = datetime.timezone(datetime.timedelta(seconds=30))
    with pytest.raises(CompileError):
        process(datetime.datetime(2026, 6, 17, 9, 57, 43, tzinfo=tz))


def test_catalogs_create_table_with_pk(dialect):
    with pytest.warns(SAWarning, match="Trino does not support PRIMARY KEY constraints. Constraint will be ignored."):
        statement = CreateTable(table_with_pk)
        query = statement.compile(dialect=dialect)
        assert 'primary key' not in str(query).lower()


def test_catalogs_create_table_with_fk(dialect):
    with pytest.warns(SAWarning, match="Trino does not support FOREIGN KEY constraints. Constraint will be ignored."):
        statement = CreateTable(table_with_fk)
        query = statement.compile(dialect=dialect)
        assert 'foreign key' not in str(query).lower()


def test_catalogs_create_table_with_unique(dialect):
    with pytest.warns(SAWarning, match="Trino does not support UNIQUE constraints. Constraint will be ignored."):
        statement = CreateTable(table_with_unique)
        query = statement.compile(dialect=dialect)
        assert 'unique' not in str(query).lower()
