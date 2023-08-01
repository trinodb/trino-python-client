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
import uuid

import pytest
import sqlalchemy as sqla
from sqlalchemy.sql import and_, not_, or_

from tests.integration.conftest import trino_version
from tests.unit.conftest import sqlalchemy_version
from trino.sqlalchemy.datatype import JSON


@pytest.fixture
def trino_connection(run_trino, request):
    _, host, port = run_trino
    connect_args = {"source": "test", "max_attempts": 1}
    if trino_version() <= '417':
        connect_args["legacy_prepared_statements"] = True
    engine = sqla.create_engine(f"trino://test@{host}:{port}/{request.param}",
                                connect_args=connect_args)
    yield engine, engine.connect()


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['tpch'], indirect=True)
def test_select_query(trino_connection):
    _, conn = trino_connection
    metadata = sqla.MetaData()
    nations = sqla.Table('nation', metadata, schema='tiny', autoload_with=conn)
    assert_column(nations, "nationkey", sqla.sql.sqltypes.BigInteger)
    assert_column(nations, "name", sqla.sql.sqltypes.String)
    assert_column(nations, "regionkey", sqla.sql.sqltypes.BigInteger)
    assert_column(nations, "comment", sqla.sql.sqltypes.String)
    query = sqla.select(nations)
    result = conn.execute(query)
    rows = result.fetchall()
    assert len(rows) == 25
    for row in rows:
        assert isinstance(row.nationkey, int)
        assert isinstance(row.name, str)
        assert isinstance(row.regionkey, int)
        assert isinstance(row.comment, str)


def assert_column(table, column_name, column_type):
    assert getattr(table.c, column_name).name == column_name
    assert isinstance(getattr(table.c, column_name).type, column_type)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['system'], indirect=True)
def test_select_specific_columns(trino_connection):
    _, conn = trino_connection
    metadata = sqla.MetaData()
    nodes = sqla.Table('nodes', metadata, schema='runtime', autoload_with=conn)
    assert_column(nodes, "node_id", sqla.sql.sqltypes.String)
    assert_column(nodes, "state", sqla.sql.sqltypes.String)
    query = sqla.select(nodes.c.node_id, nodes.c.state)
    result = conn.execute(query)
    rows = result.fetchall()
    assert len(rows) > 0
    for row in rows:
        assert isinstance(row.node_id, str)
        assert isinstance(row.state, str)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_define_and_create_table(trino_connection):
    engine, conn = trino_connection
    if not engine.dialect.has_schema(conn, "test"):
        with engine.begin() as connection:
            connection.execute(sqla.schema.CreateSchema("test"))
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


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_insert(trino_connection):
    engine, conn = trino_connection

    if not engine.dialect.has_schema(conn, "test"):
        with engine.begin() as connection:
            connection.execute(sqla.schema.CreateSchema("test"))
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


@pytest.mark.skipif(
    sqlalchemy_version() < "2.0",
    reason="sqlalchemy.Uuid only exists with SQLAlchemy 2.0 and above"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_define_and_create_table_uuid(trino_connection):
    engine, conn = trino_connection
    if not engine.dialect.has_schema(conn, "test"):
        with engine.begin() as connection:
            connection.execute(sqla.schema.CreateSchema("test"))
    metadata = sqla.MetaData()
    try:
        sqla.Table('users',
                   metadata,
                   sqla.Column('guid', sqla.Uuid),
                   schema="test")
        metadata.create_all(engine)
        assert sqla.inspect(engine).has_table('users', schema="test")
        users = sqla.Table('users', metadata, schema='test', autoload_with=conn)
        assert_column(users, "guid", sqla.sql.sqltypes.Uuid)
    finally:
        metadata.drop_all(engine)


@pytest.mark.skipif(
    sqlalchemy_version() < "2.0",
    reason="sqlalchemy.Uuid only exists with SQLAlchemy 2.0 and above"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_insert_uuid(trino_connection):
    engine, conn = trino_connection

    if not engine.dialect.has_schema(conn, "test"):
        with engine.begin() as connection:
            connection.execute(sqla.schema.CreateSchema("test"))
    metadata = sqla.MetaData()
    try:
        users = sqla.Table('users',
                           metadata,
                           sqla.Column('guid', sqla.Uuid),
                           schema="test")
        metadata.create_all(engine)
        ins = users.insert()
        guid = uuid.uuid4()
        conn.execute(ins, {"guid": guid})
        query = sqla.select(users)
        result = conn.execute(query)
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0] == (guid,)
    finally:
        metadata.drop_all(engine)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_insert_multiple_statements(trino_connection):
    engine, conn = trino_connection
    if not engine.dialect.has_schema(conn, "test"):
        with engine.begin() as connection:
            connection.execute(sqla.schema.CreateSchema("test"))
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


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
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
        assert isinstance(row.nationkey, int)
        assert isinstance(row.name, str)
        assert isinstance(row.regionkey, int)
        assert isinstance(row.comment, str)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
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
        assert isinstance(row.custkey, int)
        assert isinstance(row.name, str)
        assert isinstance(row.address, str)
        assert isinstance(row.nationkey, int)
        assert isinstance(row.phone, str)
        assert isinstance(row.acctbal, float)
        assert isinstance(row.mktsegment, str)
        assert isinstance(row.comment, str)


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
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


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
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


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
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


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
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


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize(
    'trino_connection,json_object',
    [
        ('memory', None),
        ('memory', 1),
        ('memory', 'test'),
        ('memory', [1, 'test']),
        ('memory', {'test': 1}),
    ],
    indirect=['trino_connection']
)
def test_json_column(trino_connection, json_object):
    engine, conn = trino_connection

    if not engine.dialect.has_schema(conn, "test"):
        with engine.begin() as connection:
            connection.execute(sqla.schema.CreateSchema("test"))
    metadata = sqla.MetaData()

    try:
        table_with_json = sqla.Table(
            'table_with_json',
            metadata,
            sqla.Column('id', sqla.Integer),
            sqla.Column('json_column', JSON),
            schema="test"
        )
        metadata.create_all(engine)
        ins = table_with_json.insert()
        conn.execute(ins, {"id": 1, "json_column": json_object})
        query = sqla.select(table_with_json)
        result = conn.execute(query)
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0] == (1, json_object)
    finally:
        metadata.drop_all(engine)


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_get_table_comment(trino_connection):
    engine, conn = trino_connection

    if not engine.dialect.has_schema(conn, "test"):
        with engine.begin() as connection:
            connection.execute(sqla.schema.CreateSchema("test"))
    metadata = sqla.MetaData()

    try:
        sqla.Table(
            'table_with_id',
            metadata,
            sqla.Column('id', sqla.Integer),
            schema="test",
            # comment="This is a comment" TODO: Support comment creation through sqlalchemy api
        )
        metadata.create_all(engine)
        insp = sqla.inspect(engine)
        actual = insp.get_table_comment(table_name='table_with_id', schema="test")
        assert actual['text'] is None
    finally:
        metadata.drop_all(engine)


@pytest.mark.parametrize('trino_connection', ['memory/test'], indirect=True)
@pytest.mark.parametrize('schema', [None, 'test'])
def test_get_table_names(trino_connection, schema):
    engine, conn = trino_connection
    schema_name = schema or engine.dialect._get_default_schema_name(conn)
    metadata = sqla.MetaData(schema=schema_name)

    if not engine.dialect.has_schema(conn, schema_name):
        with engine.begin() as connection:
            connection.execute(sqla.schema.CreateSchema(schema_name))

    try:
        sqla.Table(
            'test_get_table_names',
            metadata,
            sqla.Column('id', sqla.Integer),
        )
        metadata.create_all(engine)
        view_name = schema_name + ".test_view"
        conn.execute(sqla.text(f"CREATE VIEW {view_name} AS SELECT * FROM test_get_table_names"))
        assert sqla.inspect(engine).get_table_names(schema_name) == ['test_get_table_names']
    finally:
        conn.execute(sqla.text(f"DROP VIEW IF EXISTS {view_name}"))
        metadata.drop_all(engine)


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_get_table_names_raises(trino_connection):
    engine, _ = trino_connection

    with pytest.raises(sqla.exc.NoSuchTableError):
        sqla.inspect(engine).get_table_names(None)


@pytest.mark.parametrize('trino_connection', ['memory/test'], indirect=True)
@pytest.mark.parametrize('schema', [None, 'test'])
def test_get_view_names(trino_connection, schema):
    engine, conn = trino_connection
    schema_name = schema or engine.dialect._get_default_schema_name(conn)
    metadata = sqla.MetaData(schema=schema_name)

    if not engine.dialect.has_schema(conn, schema_name):
        with engine.begin() as connection:
            connection.execute(sqla.schema.CreateSchema(schema_name))

    try:
        sqla.Table(
            'test_table',
            metadata,
            sqla.Column('id', sqla.Integer),
        )
        metadata.create_all(engine)
        view_name = schema_name + ".test_get_view_names"
        conn.execute(sqla.text(f"CREATE VIEW {view_name} AS SELECT * FROM test_table"))
        assert sqla.inspect(engine).get_view_names(schema_name) == ['test_get_view_names']
    finally:
        conn.execute(sqla.text(f"DROP VIEW IF EXISTS {view_name}"))
        metadata.drop_all(engine)


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_get_view_names_raises(trino_connection):
    engine, _ = trino_connection

    with pytest.raises(sqla.exc.NoSuchTableError):
        sqla.inspect(engine).get_view_names(None)


@pytest.mark.parametrize('trino_connection', ['system'], indirect=True)
@pytest.mark.skipif(trino_version() == '351', reason="version() not supported in older Trino versions")
def test_version_is_lazy(trino_connection):
    _, conn = trino_connection
    result = conn.execute(sqla.text("SELECT 1"))
    result.fetchall()
    num_queries = _num_queries_containing_string(conn, "SELECT version()")
    assert num_queries == 0
    version_info = conn.dialect.server_version_info
    assert isinstance(version_info, tuple)
    num_queries = _num_queries_containing_string(conn, "SELECT version()")
    assert num_queries == 1


def _num_queries_containing_string(connection, query_string):
    statement = sqla.text("select query from system.runtime.queries order by query_id desc offset 1 limit 1")
    result = connection.execute(statement)
    rows = result.fetchall()
    return len(list(filter(lambda rec: query_string in rec[0], rows)))


@pytest.mark.parametrize('trino_connection', ['memory'], indirect=True)
def test_table_from_other_catalog(trino_connection):
    engine, _ = trino_connection
    t = sqla.Table(
        "catalogs",
        sqla.MetaData(),
        schema="metadata",
        trino_catalog="system",
        autoload_with=engine,
    )
    assert "system" in set(sqla.select(t.c.catalog_name).scalars())
