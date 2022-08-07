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
# limitations under the License
import pytest
import sqlalchemy as sqla
from sqlalchemy.sql import and_, or_, not_

from trino.sqlalchemy.datatype import VARCHAR


@pytest.fixture
def trino_connection(run_trino, request):
    _, host, port = run_trino
    engine = sqla.create_engine(f"trino://test@{host}:{port}/{request.param}",
                                connect_args={"source": "test", "max_attempts": 1})
    yield engine, engine.connect()


@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
def test_select_query(trino_connection):
    _, conn = trino_connection
    metadata = sqla.MetaData()
    nations = sqla.Table('nation', metadata, schema='tiny', autoload_with=conn)
    assert_column(nations, "nationkey", sqla.sql.sqltypes.BigInteger)
    assert_column(nations, "name", VARCHAR)
    assert_column(nations, "regionkey", sqla.sql.sqltypes.BigInteger)
    assert_column(nations, "comment", VARCHAR)
    query = sqla.select(nations)
    result = conn.execute(query)
    rows = result.fetchall()
    assert len(rows) == 25
    for row in rows:
        assert isinstance(row['nationkey'], int)
        assert isinstance(row['name'], str)
        assert isinstance(row['regionkey'], int)
        assert isinstance(row['comment'], str)


def assert_column(table, column_name, column_type):
    assert getattr(table.c, column_name).name == column_name
    assert isinstance(getattr(table.c, column_name).type, column_type)


@pytest.mark.parametrize('trino_connection', ['system'], indirect=True)
def test_select_specific_columns(trino_connection):
    _, conn = trino_connection
    metadata = sqla.MetaData()
    nodes = sqla.Table('nodes', metadata, schema='runtime', autoload_with=conn)
    assert_column(nodes, "node_id", VARCHAR)
    assert_column(nodes, "state", VARCHAR)
    query = sqla.select(nodes.c.node_id, nodes.c.state)
    result = conn.execute(query)
    rows = result.fetchall()
    assert len(rows) > 0
    for row in rows:
        assert isinstance(row['node_id'], str)
        assert isinstance(row['state'], str)


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_define_and_create_table(trino_connection):
    engine, conn = trino_connection
    if not engine.dialect.has_schema(engine, "test"):
        engine.execute(sqla.schema.CreateSchema("test"))
    metadata = sqla.MetaData()
    try:
        sqla.Table('users',
                   metadata,
                   sqla.Column('id', sqla.Integer),
                   sqla.Column('name', sqla.String),
                   sqla.Column('fullname', sqla.String),
                   schema="test")
        metadata.create_all(engine)
        assert sqla.inspect(engine).has_table('users', schema="test")
        users = sqla.Table('users', metadata, schema='test', autoload_with=conn)
        assert_column(users, "id", sqla.sql.sqltypes.Integer)
        assert_column(users, "name", sqla.sql.sqltypes.String)
        assert_column(users, "fullname", sqla.sql.sqltypes.String)
    finally:
        metadata.drop_all(engine)


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_insert(trino_connection):
    engine, conn = trino_connection

    if not engine.dialect.has_schema(engine, "test"):
        engine.execute(sqla.schema.CreateSchema("test"))
    metadata = sqla.MetaData()
    try:
        users = sqla.Table('users',
                           metadata,
                           sqla.Column('id', sqla.Integer),
                           sqla.Column('name', sqla.String),
                           sqla.Column('fullname', sqla.String),
                           schema="test")
        metadata.create_all(engine)
        ins = users.insert()
        conn.execute(ins, {"id": 2, "name": "wendy", "fullname": "Wendy Williams"})
        query = sqla.select(users)
        result = conn.execute(query)
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0] == (2, "wendy", "Wendy Williams")
    finally:
        metadata.drop_all(engine)


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_insert_multiple_statements(trino_connection):
    engine, conn = trino_connection
    if not engine.dialect.has_schema(engine, "test"):
        engine.execute(sqla.schema.CreateSchema("test"))
    metadata = sqla.MetaData()
    users = sqla.Table('users',
                       metadata,
                       sqla.Column('id', sqla.Integer),
                       sqla.Column('name', sqla.String),
                       sqla.Column('fullname', sqla.String),
                       schema="test")
    metadata.create_all(engine)
    ins = users.insert()
    conn.execute(ins, [
        {"id": 2, "name": "wendy", "fullname": "Wendy Williams"},
        {"id": 3, "name": "john", "fullname": "John Doe"},
        {"id": 4, "name": "mary", "fullname": "Mary Hopkins"},
    ])
    query = sqla.select(users)
    result = conn.execute(query)
    rows = result.fetchall()
    assert len(rows) == 3
    assert frozenset(rows) == frozenset([
        (2, "wendy", "Wendy Williams"),
        (3, "john", "John Doe"),
        (4, "mary", "Mary Hopkins"),
    ])
    metadata.drop_all(engine)


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_insert_multiple_statements_number_to_string_coercion(trino_connection):
    engine, conn = trino_connection
    if not engine.dialect.has_schema(engine, "test"):
        engine.execute(sqla.schema.CreateSchema("test"))
    metadata = sqla.MetaData()
    users = sqla.Table('users',
                       metadata,
                       sqla.Column('id', sqla.Integer),
                       sqla.Column('name', sqla.String),
                       sqla.Column('fullname', sqla.String),
                       schema="test")
    metadata.create_all(engine)
    ins = users.insert()
    conn.execute(ins, [
        {"id": 2, "name": "wendy", "fullname": "Wendy Williams"},
        {"id": 3, "name": "john", "fullname": 1234},
        {"id": 4, "name": "mary", "fullname": "Mary Hopkins"},
    ])
    query = sqla.select(users)
    result = conn.execute(query)
    rows = result.fetchall()
    assert len(rows) == 3
    assert frozenset(rows) == frozenset([
        (2, "wendy", "Wendy Williams"),
        (3, "john", "1234"),
        (4, "mary", "Mary Hopkins"),
    ])
    metadata.drop_all(engine)


@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
def test_operators(trino_connection):
    _, conn = trino_connection
    metadata = sqla.MetaData()
    customers = sqla.Table('nation', metadata, schema='tiny', autoload_with=conn)
    query = sqla.select(customers).where(customers.c.nationkey == 2)
    result = conn.execute(query)
    rows = result.fetchall()
    assert len(rows) == 1
    for row in rows:
        assert isinstance(row['nationkey'], int)
        assert isinstance(row['name'], str)
        assert isinstance(row['regionkey'], int)
        assert isinstance(row['comment'], str)


@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
def test_conjunctions(trino_connection):
    _, conn = trino_connection
    metadata = sqla.MetaData()
    customers = sqla.Table('customer', metadata, schema='tiny', autoload_with=conn)
    query = sqla.select(customers).where(and_(
        customers.c.name.like('%12%'),
        customers.c.nationkey == 15,
        or_(
            customers.c.mktsegment == 'AUTOMOBILE',
            customers.c.mktsegment == 'HOUSEHOLD'
        ),
        not_(customers.c.acctbal < 0)))
    result = conn.execute(query)
    rows = result.fetchall()
    assert len(rows) == 1


@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
def test_textual_sql(trino_connection):
    _, conn = trino_connection
    s = sqla.text("SELECT * from tiny.customer where nationkey = :e1 AND acctbal < :e2")
    result = conn.execute(s, {"e1": 15, "e2": 0})
    rows = result.fetchall()
    assert len(rows) == 3
    for row in rows:
        assert isinstance(row['custkey'], int)
        assert isinstance(row['name'], str)
        assert isinstance(row['address'], str)
        assert isinstance(row['nationkey'], int)
        assert isinstance(row['phone'], str)
        assert isinstance(row['acctbal'], float)
        assert isinstance(row['mktsegment'], str)
        assert isinstance(row['comment'], str)


@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
def test_alias(trino_connection):
    _, conn = trino_connection
    metadata = sqla.MetaData()
    nations = sqla.Table('nation', metadata, schema='tiny', autoload_with=conn)
    nations1 = nations.alias("o1")
    nations2 = nations.alias("o2")
    s = sqla.select(nations1) \
        .join(nations2, and_(
            nations1.c.regionkey == nations2.c.regionkey,
            nations1.c.nationkey != nations2.c.nationkey,
            nations1.c.regionkey == 1
        )) \
        .distinct()
    result = conn.execute(s)
    rows = result.fetchall()
    assert len(rows) == 5


@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
def test_subquery(trino_connection):
    _, conn = trino_connection
    metadata = sqla.MetaData()
    nations = sqla.Table('nation', metadata, schema='tiny', autoload_with=conn)
    customers = sqla.Table('customer', metadata, schema='tiny', autoload_with=conn)
    automobile_customers = sqla.select(customers.c.nationkey).where(customers.c.acctbal < -900)
    automobile_customers_subquery = automobile_customers.subquery()
    s = sqla.select(nations.c.name).where(nations.c.nationkey.in_(sqla.select(automobile_customers_subquery)))
    result = conn.execute(s)
    rows = result.fetchall()
    assert len(rows) == 15


@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
def test_joins(trino_connection):
    _, conn = trino_connection
    metadata = sqla.MetaData()
    nations = sqla.Table('nation', metadata, schema='tiny', autoload_with=conn)
    customers = sqla.Table('customer', metadata, schema='tiny', autoload_with=conn)
    s = sqla.select(nations.c.name) \
        .select_from(nations.join(customers, nations.c.nationkey == customers.c.nationkey)) \
        .where(customers.c.acctbal < -900) \
        .distinct()
    result = conn.execute(s)
    rows = result.fetchall()
    assert len(rows) == 15


@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
def test_cte(trino_connection):
    _, conn = trino_connection
    metadata = sqla.MetaData()
    nations = sqla.Table('nation', metadata, schema='tiny', autoload_with=conn)
    customers = sqla.Table('customer', metadata, schema='tiny', autoload_with=conn)
    automobile_customers = sqla.select(customers.c.nationkey).where(customers.c.acctbal < -900)
    automobile_customers_cte = automobile_customers.cte()
    s = sqla.select(nations).where(nations.c.nationkey.in_(sqla.select(automobile_customers_cte)))
    result = conn.execute(s)
    rows = result.fetchall()
    assert len(rows) == 15
