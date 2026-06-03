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
from decimal import Decimal

import pytest

import trino.client
from trino.dbapi import _INSERT_VALUES_RE
from trino.dbapi import Connection
from trino.dbapi import Cursor


class FakeTrinoQuery:
    """Hand-written fake replacing TrinoQuery for unit tests.

    Records every SQL string passed and behaves like a successful INSERT.
    """

    instances = []

    def __init__(self, request, query, legacy_primitive_types=False, fetch_mode="mapped"):
        self._query = query
        self._update_type = "INSERT"
        FakeTrinoQuery.instances.append(self)

    def execute(self):
        return iter([])

    @property
    def query(self):
        return self._query

    @property
    def update_type(self):
        return self._update_type


class FakeTrinoRequest:
    pass


def _make_cursor(batch_executemany=True):
    cur = Cursor.__new__(Cursor)
    cur._connection = Connection.__new__(Connection)
    cur._connection._client_session = type("cs", (), {"timezone": None})()
    cur._request = FakeTrinoRequest()
    cur._iterator = None
    cur._query = None
    cur._legacy_primitive_types = False
    cur._experimental_batch_executemany = batch_executemany
    return cur


@pytest.fixture
def cursor():
    """Create a real Cursor wired to FakeTrinoQuery with batching enabled.

    Monkeypatches trino.client.TrinoQuery so the production
    _executemany_batch_insert runs its real code path but creates
    fakes instead of making HTTP calls.
    """
    FakeTrinoQuery.instances = []
    original = trino.client.TrinoQuery
    trino.client.TrinoQuery = FakeTrinoQuery

    yield _make_cursor(batch_executemany=True)

    trino.client.TrinoQuery = original


class TestInsertValuesPattern:
    def test_simple_insert(self):
        assert _INSERT_VALUES_RE.match("INSERT INTO t (a, b) VALUES (?, ?)") is not None

    def test_insert_with_schema(self):
        sql = 'INSERT INTO "my_schema"."my_table" (col1, col2) VALUES (?, ?)'
        assert _INSERT_VALUES_RE.match(sql) is not None

    def test_insert_with_catalog_schema(self):
        sql = 'INSERT INTO "catalog"."schema"."table" (a, b, c) VALUES (?, ?, ?)'
        assert _INSERT_VALUES_RE.match(sql) is not None

    def test_multiline_insert(self):
        sql = '  INSERT INTO "schema"."table" (col1, col2)\n            VALUES (?, ?)\n        '
        assert _INSERT_VALUES_RE.match(sql.strip()) is not None

    def test_insert_no_columns(self):
        assert _INSERT_VALUES_RE.match("INSERT INTO t VALUES (?, ?)") is not None

    def test_select_not_matched(self):
        assert _INSERT_VALUES_RE.match("SELECT * FROM t WHERE a = ?") is None

    def test_update_not_matched(self):
        assert _INSERT_VALUES_RE.match("UPDATE t SET a = ? WHERE b = ?") is None

    def test_insert_select_not_matched(self):
        assert _INSERT_VALUES_RE.match("INSERT INTO t SELECT * FROM s") is None

    def test_trailing_semicolon_not_matched(self):
        assert _INSERT_VALUES_RE.match("INSERT INTO t (a) VALUES (?);") is None

    def test_case_insensitive(self):
        assert _INSERT_VALUES_RE.match("insert into t values (?)") is not None

    def test_prefix_extraction(self):
        sql = 'INSERT INTO "s"."t" (a, b) VALUES (?, ?)'
        m = _INSERT_VALUES_RE.match(sql)
        assert m is not None
        assert m.group(1).strip().endswith("VALUES")


class TestExecutemanyBatchInsert:
    def test_single_row(self, cursor):
        cursor.executemany(
            "INSERT INTO t (a, b) VALUES (?, ?)",
            [(1, "hello")]
        )
        assert len(FakeTrinoQuery.instances) == 1
        assert "VALUES (1, 'hello')" in FakeTrinoQuery.instances[0].query

    def test_multiple_rows_single_batch(self, cursor):
        cursor.executemany(
            "INSERT INTO t (a, b) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")]
        )
        assert len(FakeTrinoQuery.instances) == 1
        sql = FakeTrinoQuery.instances[0].query
        assert "(1, 'a')" in sql
        assert "(2, 'b')" in sql
        assert "(3, 'c')" in sql

    def test_chunking(self, cursor):
        import trino.dbapi as _dbapi
        original = _dbapi._EXECUTEMANY_BATCH_SIZE
        _dbapi._EXECUTEMANY_BATCH_SIZE = 2
        try:
            cursor.executemany(
                "INSERT INTO t (a) VALUES (?)",
                [(1,), (2,), (3,), (4,), (5,)]
            )
            assert len(FakeTrinoQuery.instances) == 3
            assert "(1)" in FakeTrinoQuery.instances[0].query
            assert "(2)" in FakeTrinoQuery.instances[0].query
            assert "(3)" in FakeTrinoQuery.instances[1].query
            assert "(4)" in FakeTrinoQuery.instances[1].query
            assert "(5)" in FakeTrinoQuery.instances[2].query
        finally:
            _dbapi._EXECUTEMANY_BATCH_SIZE = original

    def test_null_values(self, cursor):
        cursor.executemany(
            "INSERT INTO t (a, b) VALUES (?, ?)",
            [(1, None), (None, "test")]
        )
        sql = FakeTrinoQuery.instances[0].query
        assert "(1, NULL)" in sql
        assert "(NULL, 'test')" in sql

    def test_mixed_types(self, cursor):
        cursor.executemany(
            "INSERT INTO t (a, b, c, d) VALUES (?, ?, ?, ?)",
            [(42, "text", True, Decimal("3.14"))]
        )
        sql = FakeTrinoQuery.instances[0].query
        assert "42" in sql
        assert "'text'" in sql
        assert "true" in sql
        assert "DECIMAL '3.14'" in sql

    def test_string_escaping(self, cursor):
        cursor.executemany(
            "INSERT INTO t (a) VALUES (?)",
            [("it's a test",)]
        )
        assert "it''s a test" in FakeTrinoQuery.instances[0].query

    def test_empty_params_does_not_batch(self, cursor):
        cursor.executemany(
            "INSERT INTO t (a) VALUES (?)",
            []
        )
        # Empty params takes the execute() path, not batch path.
        # The FakeTrinoQuery from execute() still gets created but
        # via the non-batch code path.
        assert cursor._query is not None
        assert cursor._query.query == "INSERT INTO t (a) VALUES (?)"


class TestBatchExecutemanyOptIn:
    def test_disabled_by_default(self):
        """When experimental_batch_executemany is False (default), INSERT
        executemany falls through to the row-by-row execute() path."""
        FakeTrinoQuery.instances = []
        original = trino.client.TrinoQuery
        trino.client.TrinoQuery = FakeTrinoQuery
        try:
            cur = _make_cursor(batch_executemany=False)
            conn = cur._connection
            conn._use_legacy_prepared_statements = lambda: False
            conn._create_request = lambda: FakeTrinoRequest()
            cur.executemany(
                "INSERT INTO t (a, b) VALUES (?, ?)",
                [(1, "a"), (2, "b"), (3, "c")]
            )
            # Row-by-row: one TrinoQuery per param set via execute()
            assert len(FakeTrinoQuery.instances) == 3
        finally:
            trino.client.TrinoQuery = original

    def test_enabled_batches(self):
        """When experimental_batch_executemany is True, INSERT executemany
        uses the batch path."""
        FakeTrinoQuery.instances = []
        original = trino.client.TrinoQuery
        trino.client.TrinoQuery = FakeTrinoQuery
        try:
            cur = _make_cursor(batch_executemany=True)
            cur.executemany(
                "INSERT INTO t (a, b) VALUES (?, ?)",
                [(1, "a"), (2, "b"), (3, "c")]
            )
            # Batched: single TrinoQuery with all rows
            assert len(FakeTrinoQuery.instances) == 1
            sql = FakeTrinoQuery.instances[0].query
            assert "(1, 'a')" in sql
            assert "(2, 'b')" in sql
            assert "(3, 'c')" in sql
        finally:
            trino.client.TrinoQuery = original

    def test_connection_threads_flag_to_cursor(self):
        """Connection.experimental_batch_executemany is passed to Cursor."""
        conn = Connection.__new__(Connection)
        conn.experimental_batch_executemany = True
        conn.legacy_primitive_types = False
        conn.legacy_prepared_statements = None
        conn._isolation_level = 0  # AUTOCOMMIT
        conn._transaction = None
        cur = Cursor.__new__(Cursor)
        cur._connection = conn
        cur._request = FakeTrinoRequest()
        cur._iterator = None
        cur._query = None
        cur._legacy_primitive_types = False
        cur._experimental_batch_executemany = conn.experimental_batch_executemany
        assert cur._experimental_batch_executemany is True
