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
from sqlalchemy import (
    Column,
    insert,
    Integer,
    MetaData,
    select,
    String,
    Table,
)
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import column, table

from tests.unit.conftest import sqlalchemy_version
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
