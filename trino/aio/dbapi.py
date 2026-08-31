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
"""

Asynchronous, DBAPI-like interface to Trino built on ``httpx2.AsyncClient``.

The API mirrors :mod:`trino.dbapi` with coroutine methods, but it is not a
PEP 249 implementation (PEP 249 is a synchronous specification):

::

    import trino.aio

    conn = trino.aio.connect(host="coordinator", port=8080, user="user")
    cur = conn.cursor()
    await cur.execute("SELECT * FROM system.runtime.nodes")
    rows = await cur.fetchall()
    await conn.close()

Transactions are not supported by the asynchronous client yet.
"""
import asyncio
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union
from urllib.parse import urlparse

import httpx2

import trino.client
import trino.exceptions
import trino.logging
from trino import constants
from trino.aio.client import AsyncTrinoQuery
from trino.aio.client import AsyncTrinoRequest
from trino.dbapi import _default_spooling_encoding
from trino.dbapi import _require_tls_for_auth
from trino.dbapi import _resolve_scheme_and_port
from trino.dbapi import _USE_DEFAULT_ENCODING
from trino.dbapi import Cursor as _SyncCursor
from trino.dbapi import DescribeOutput
from trino.dbapi import must_use_legacy_prepared_statements
from trino.exceptions import NotSupportedError
from trino.transaction import IsolationLevel
from trino.transaction import NO_TRANSACTION

__all__ = ["connect", "Connection", "Cursor", "SegmentCursor"]

logger = trino.logging.get_logger(__name__)


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database.

    See class :py:class:`Connection` for arguments. No I/O happens until the
    first statement is executed.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


class Connection:
    """Asynchronous Trino connection. Accepts the same arguments as
    :class:`trino.dbapi.Connection` except that transactions (a non-default
    ``isolation_level``) are not supported yet."""

    def __init__(
        self,
        host: str,
        port=None,
        user=None,
        source=constants.DEFAULT_SOURCE,
        catalog=constants.DEFAULT_CATALOG,
        schema=constants.DEFAULT_SCHEMA,
        session_properties=None,
        http_headers=None,
        http_scheme=None,
        auth=constants.DEFAULT_AUTH,
        extra_credential=None,
        max_attempts=constants.DEFAULT_MAX_ATTEMPTS,
        request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,
        isolation_level=IsolationLevel.AUTOCOMMIT,
        verify=True,
        http_session: Optional[httpx2.AsyncClient] = None,
        client_tags=None,
        legacy_primitive_types=False,
        legacy_prepared_statements=None,
        roles=None,
        timezone=None,
        encoding: Union[str, List[str]] = _USE_DEFAULT_ENCODING,
        heartbeat_interval: Optional[float] = constants.DEFAULT_HEARTBEAT_INTERVAL,
        allow_insecure_auth: bool = False,
    ):
        if isolation_level != IsolationLevel.AUTOCOMMIT:
            raise NotSupportedError("transactions are not supported by the asynchronous client yet")

        # Automatically assign http_schema, port based on hostname
        parsed_host = urlparse(host, allow_fragments=False)

        if encoding is _USE_DEFAULT_ENCODING:
            encoding = _default_spooling_encoding()

        self.host = host if parsed_host.hostname is None else parsed_host.hostname + parsed_host.path
        self.user = user
        self.source = source
        self.catalog = catalog
        self.schema = schema
        self.session_properties = session_properties
        self._client_session = trino.client.ClientSession(
            user=user,
            catalog=catalog,
            schema=schema,
            source=source,
            properties=session_properties,
            headers=http_headers,
            transaction_id=NO_TRANSACTION,
            extra_credential=extra_credential,
            client_tags=client_tags,
            roles=roles,
            timezone=timezone,
            encoding=encoding,
            heartbeat_interval=heartbeat_interval,
        )
        if http_session is None:
            self._http_session = AsyncTrinoRequest.create_http_client(
                verify=verify, timeout=request_timeout, auth=auth
            )
        else:
            self._http_session = http_session
        self.http_headers = http_headers

        self.http_scheme, self.port = _resolve_scheme_and_port(parsed_host, port, http_scheme)
        _require_tls_for_auth(auth, self.http_scheme, allow_insecure_auth)

        self.auth = auth
        self.extra_credential = extra_credential
        self.max_attempts = max_attempts
        self.request_timeout = request_timeout
        self.client_tags = client_tags

        self._isolation_level = isolation_level
        self.legacy_primitive_types = legacy_primitive_types
        self.legacy_prepared_statements = legacy_prepared_statements

        # Pending fire-and-forget tasks (spooled segment acknowledgments) and
        # the requests spawned by this connection, both drained on close().
        self._task_registry: Set[asyncio.Task] = set()
        self._requests: List[AsyncTrinoRequest] = []

    @property
    def isolation_level(self):
        return self._isolation_level

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def close(self):
        for request in self._requests:
            await request.drain()
        await self._http_session.aclose()

    def start_transaction(self):
        raise NotSupportedError("transactions are not supported by the asynchronous client yet")

    async def commit(self):
        # Autocommit is the only supported mode; there is nothing to commit.
        pass

    async def rollback(self):
        raise trino.exceptions.NotSupportedError(
            "transactions are not supported by the asynchronous client yet")

    def _create_request(self) -> AsyncTrinoRequest:
        request = AsyncTrinoRequest(
            self.host,
            self.port,
            self._client_session,
            self._http_session,
            self.http_scheme,
            self.auth,
            self.max_attempts,
            self.request_timeout,
            task_registry=self._task_registry,
        )
        self._requests.append(request)
        return request

    def cursor(
            self,
            cursor_style: str = "row",
            legacy_primitive_types: bool = None,
            stats_callback: Optional[Callable[[Dict[str, Any]], None]] = None):
        """Return a new :py:class:`Cursor` object using the connection."""
        cursor_class = {
            # Add any custom Cursor classes here
            "segment": SegmentCursor,
            "row": Cursor
        }.get(cursor_style.lower(), Cursor)

        return cursor_class(
            self,
            self._create_request(),
            legacy_primitive_types=(
                legacy_primitive_types
                if legacy_primitive_types is not None
                else self.legacy_primitive_types
            ),
            stats_callback=stats_callback
        )

    async def _use_legacy_prepared_statements(self):
        if self.legacy_prepared_statements is not None:
            return self.legacy_prepared_statements

        value = must_use_legacy_prepared_statements.get((self.host, self.port))
        if value is None:
            try:
                query = AsyncTrinoQuery(
                    self._create_request(),
                    query="EXECUTE IMMEDIATE 'SELECT 1'")
                await query.execute()
                value = False
            except Exception as e:
                logger.warning(
                    "EXECUTE IMMEDIATE not available for %s:%s; defaulting to legacy prepared statements (%s)",
                    self.host, self.port, e)
                value = True
            must_use_legacy_prepared_statements.put((self.host, self.port), value)
        return value


class Cursor(_SyncCursor):
    """Asynchronous database cursor. Shares the parameter formatting and
    description logic with :class:`trino.dbapi.Cursor`; the execute and fetch
    methods are coroutines and rows are iterated with ``async for``."""

    def __init__(
            self,
            connection,
            request: AsyncTrinoRequest,
            legacy_primitive_types: bool = False,
            stats_callback: Optional[Callable[[Dict[str, Any]], None]] = None):
        if not isinstance(connection, Connection):
            raise ValueError(
                "connection must be a trino.aio Connection object: {}".format(type(connection))
            )
        self._connection = connection
        self._request = request

        self.arraysize = 1
        self._iterator = None
        self._query = None
        self._legacy_primitive_types = legacy_primitive_types
        self._stats_callback = stats_callback

    def __iter__(self):
        raise TypeError("use 'async for' to iterate an asynchronous cursor")

    def __aiter__(self):
        return self._iterator

    def __enter__(self):
        raise TypeError("use 'async with' with an asynchronous cursor")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def _prepare_statement(self, statement: str, name: str) -> None:
        sql = f"PREPARE {name} FROM {statement}"
        query = AsyncTrinoQuery(self.connection._create_request(), query=sql,
                                legacy_primitive_types=self._legacy_primitive_types)
        await query.execute()

    def _execute_prepared_statement(
        self,
        statement_name,
        params
    ):
        sql = 'EXECUTE ' + statement_name + ' USING ' + ','.join(map(self._format_prepared_param, params))
        return AsyncTrinoQuery(
            self._request,
            query=sql,
            legacy_primitive_types=self._legacy_primitive_types,
            stats_callback=self._stats_callback)

    def _execute_immediate_statement(self, statement: str, params):
        sql = "EXECUTE IMMEDIATE '" + statement.replace("'", "''") + \
              "' USING " + ",".join(map(self._format_prepared_param, params))
        return AsyncTrinoQuery(
            self.connection._create_request(),
            query=sql,
            legacy_primitive_types=self._legacy_primitive_types,
            stats_callback=self._stats_callback)

    async def _deallocate_prepared_statement(self, statement_name: str) -> None:
        sql = 'DEALLOCATE PREPARE ' + statement_name
        query = AsyncTrinoQuery(self.connection._create_request(), query=sql,
                                legacy_primitive_types=self._legacy_primitive_types)
        await query.execute()

    async def execute(self, operation, params=None):
        if params:
            assert isinstance(params, (list, tuple)), (
                'params must be a list or tuple containing the query '
                'parameter values'
            )

            if await self.connection._use_legacy_prepared_statements():
                statement_name = self._generate_unique_statement_name()
                await self._prepare_statement(operation, statement_name)

                try:
                    # Send execute statement and assign the return value to `results`
                    # as it will be returned by the function
                    self._query = self._execute_prepared_statement(
                        statement_name, params
                    )
                    self._iterator = (await self._query.execute()).__aiter__()
                finally:
                    # Send deallocate statement
                    # At this point the query can be deallocated since it has already
                    # been executed
                    await self._deallocate_prepared_statement(statement_name)
            else:
                self._query = self._execute_immediate_statement(operation, params)
                self._iterator = (await self._query.execute()).__aiter__()

        else:
            self._query = AsyncTrinoQuery(self._request, query=operation,
                                          legacy_primitive_types=self._legacy_primitive_types,
                                          stats_callback=self._stats_callback)
            self._iterator = (await self._query.execute()).__aiter__()
        return self

    async def executemany(self, operation, seq_of_params):
        """
        Prepare a database operation (query or command) and then execute it
        against all parameter sequences found in ``seq_of_params``; see
        :meth:`trino.dbapi.Cursor.executemany`.
        """
        for parameters in seq_of_params[:-1]:
            await self.execute(operation, parameters)
            await self.fetchall()
            if self._query.update_type is None:
                raise NotSupportedError("Query must return update type")
        if seq_of_params:
            await self.execute(operation, seq_of_params[-1])
        else:
            await self.execute(operation)
        return self

    async def fetchone(self) -> Optional[List[Any]]:
        """
        Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.
        """
        try:
            assert self._iterator is not None
            return await self._iterator.__anext__()
        except StopAsyncIteration:
            return None
        except trino.exceptions.HttpError as err:
            raise trino.exceptions.OperationalError(str(err))

    async def fetchmany(self, size=None) -> List[List[Any]]:
        """
        Fetch the next set of rows of a query result, returning a list of
        rows. An empty list is returned when no more rows are available.
        """
        if size is None:
            size = self.arraysize

        rows = []
        for _ in range(size):
            row = await self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    async def describe(self, sql: str) -> List[DescribeOutput]:
        """
        List the output columns of a SQL statement; see
        :meth:`trino.dbapi.Cursor.describe`.
        """
        statement_name = self._generate_unique_statement_name()
        await self._prepare_statement(sql, statement_name)
        try:
            sql = f"DESCRIBE OUTPUT {statement_name}"
            self._query = AsyncTrinoQuery(
                self._request,
                query=sql,
                legacy_primitive_types=self._legacy_primitive_types,
            )
            result = await self._query.execute()
            rows = [row async for row in result]
        finally:
            await self._deallocate_prepared_statement(statement_name)

        return list(map(lambda x: DescribeOutput.from_row(x), rows))

    async def fetchall(self) -> List[List[Any]]:
        rows = []
        while True:
            row = await self.fetchone()
            if row is None:
                return rows
            rows.append(row)

    async def cancel(self):
        if self._query is None:
            return
        await self._query.cancel()

    async def close(self):
        await self.cancel()
        # TODO: Cancel not only the last query executed on this cursor
        #  but also any other outstanding queries executed through this cursor.


class SegmentCursor(Cursor):
    def __init__(
            self,
            connection,
            request: AsyncTrinoRequest,
            legacy_primitive_types: bool = False,
            stats_callback: Optional[Callable[[Dict[str, Any]], None]] = None):
        super().__init__(
            connection, request, legacy_primitive_types=legacy_primitive_types, stats_callback=stats_callback)
        if self.connection._client_session.encoding is None:
            raise ValueError("SegmentCursor can only be used if encoding is set on the connection")

    async def execute(self, operation, params=None):
        if params:
            # TODO: refactor code to allow for params to be supported
            raise ValueError("params not supported")

        self._query = AsyncTrinoQuery(self._request, query=operation,
                                      legacy_primitive_types=self._legacy_primitive_types,
                                      fetch_mode="segments",
                                      stats_callback=self._stats_callback)
        self._iterator = (await self._query.execute()).__aiter__()
        return self
