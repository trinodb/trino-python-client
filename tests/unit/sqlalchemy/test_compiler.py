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
from sqlalchemy import Table, MetaData, Column, Integer, String, select

from trino.sqlalchemy.dialect import TrinoDialect

metadata = MetaData()
table = Table(
    'table',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
)


@pytest.fixture
def dialect():
    return TrinoDialect()


def test_limit_offset(dialect):
    statement = select(table).limit(10).offset(0)
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nOFFSET :param_1\nLIMIT :param_2'


def test_limit(dialect):
    statement = select(table).limit(10)
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nLIMIT :param_1'


def test_offset(dialect):
    statement = select(table).offset(0)
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nOFFSET :param_1'
