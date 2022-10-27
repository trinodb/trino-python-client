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

This module implements the Python DBAPI 2.0 as described in
https://www.python.org/dev/peps/pep-0249/ .

Fetch methods returns rows as a list of lists on purpose to let the caller
decide to convert then to a list of tuples.
"""
import datetime
import math
import uuid
from decimal import Decimal
from typing import Any, List, Optional  # NOQA for mypy types

import trino.client
import trino.exceptions
import trino.logging
from trino import constants
from trino.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from trino.transaction import NO_TRANSACTION, IsolationLevel, Transaction

__all__ = [
    # https://www.python.org/dev/peps/pep-0249/#globals
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Cursor",
    # https://www.python.org/dev/peps/pep-0249/#exceptions
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]


apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"

logger = trino.logging.get_logger(__name__)


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database.

    See class :py:class:`Connection` for arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    """Trino supports transactions and the ability to either commit or rollback
    a sequence of SQL statements. A single query i.e. the execution of a SQL
    statement, can also be cancelled. Transactions are not supported by this
    client implementation yet.

    """

    def __init__(
        self,
        host,
        port=constants.DEFAULT_PORT,
        user=None,
        source=constants.DEFAULT_SOURCE,
        catalog=constants.DEFAULT_CATALOG,
        schema=constants.DEFAULT_SCHEMA,
        session_properties=None,
        http_headers=None,
        http_scheme=constants.HTTP,
        auth=constants.DEFAULT_AUTH,
        extra_credential=None,
        redirect_handler=None,
        max_attempts=constants.DEFAULT_MAX_ATTEMPTS,
        request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,
        isolation_level=IsolationLevel.AUTOCOMMIT,
        verify=True,
        http_session=None,
        client_tags=None,
        experimental_python_types=False,
        roles=None,
    ):
        self.host = host
        self.port = port
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
        )
        # mypy cannot follow module import
        if http_session is None:
            self._http_session = trino.client.TrinoRequest.http.Session()
            self._http_session.verify = verify
        else:
            self._http_session = http_session
        self.http_headers = http_headers
        self.http_scheme = http_scheme
        self.auth = auth
        self.extra_credential = extra_credential
        self.redirect_handler = redirect_handler
        self.max_attempts = max_attempts
        self.request_timeout = request_timeout
        self.client_tags = client_tags

        self._isolation_level = isolation_level
        self._request = None
        self._transaction = None
        self.experimental_python_types = experimental_python_types

    @property
    def isolation_level(self):
        return self._isolation_level

    @property
    def transaction(self):
        return self._transaction

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.commit()
        except Exception:
            self.rollback()
        else:
            self.close()

    def close(self):
        """Trino does not have anything to close"""
        # TODO cancel outstanding queries?
        pass

    def start_transaction(self):
        self._transaction = Transaction(self._create_request())
        self._transaction.begin()
        return self._transaction

    def commit(self):
        if self.transaction is None:
            return
        self._transaction.commit()
        self._transaction = None

    def rollback(self):
        if self.transaction is None:
            raise RuntimeError("no transaction was started")
        self._transaction.rollback()
        self._transaction = None

    def _create_request(self):
        return trino.client.TrinoRequest(
            self.host,
            self.port,
            self._client_session,
            self._http_session,
            self.http_scheme,
            self.auth,
            self.redirect_handler,
            self.max_attempts,
            self.request_timeout,
        )

    def cursor(self, experimental_python_types: bool = None):
        """Return a new :py:class:`Cursor` object using the connection."""
        if self.isolation_level != IsolationLevel.AUTOCOMMIT:
            if self.transaction is None:
                self.start_transaction()
        if self.transaction is not None:
            request = self.transaction.request
        else:
            request = self._create_request()
        return Cursor(
            self,
            request,
            # if experimental_python_types is not explicitly set in Cursor, take from Connection
            experimental_python_types if experimental_python_types is not None else self.experimental_python_types
        )


class Cursor(object):
    """Database cursor.

    Cursors are not isolated, i.e., any changes done to the database by a
    cursor are immediately visible by other cursors or connections.

    """

    def __init__(self, connection, request, experimental_python_types: bool = False):
        if not isinstance(connection, Connection):
            raise ValueError(
                "connection must be a Connection object: {}".format(type(connection))
            )
        self._connection = connection
        self._request = request

        self.arraysize = 1
        self._iterator = None
        self._query = None
        self._experimental_pyton_types = experimental_python_types

    def __iter__(self):
        return self._iterator

    @property
    def connection(self):
        return self._connection

    @property
    def info_uri(self):
        if self._query is not None:
            return self._query.info_uri
        return None

    @property
    def update_type(self):
        if self._query is not None:
            return self._query.update_type
        return None

    @property
    def description(self):
        if self._query.columns is None:
            return None

        # [ (name, type_code, display_size, internal_size, precision, scale, null_ok) ]
        return [
            (col["name"], col["type"], None, None, None, None, None)
            for col in self._query.columns
        ]

    @property
    def rowcount(self):
        """Not supported.

        Trino cannot reliablity determine the number of rows returned by an
        operation. For example, the result of a SELECT query is streamed and
        the number of rows is only knowns when all rows have been retrieved.
        """

        return -1

    @property
    def stats(self):
        if self._query is not None:
            return self._query.stats
        return None

    @property
    def warnings(self):
        if self._query is not None:
            return self._query.warnings
        return None

    def setinputsizes(self, sizes):
        raise trino.exceptions.NotSupportedError

    def setoutputsize(self, size, column):
        raise trino.exceptions.NotSupportedError

    def _prepare_statement(self, statement: str, name: str) -> None:
        """
        Registers a prepared statement for the provided `operation` with the
        `name` assigned to it.

        :param statement: sql to be executed.
        :param name: name that will be assigned to the prepared statement.
        """
        sql = f"PREPARE {name} FROM {statement}"
        query = trino.client.TrinoQuery(self.connection._create_request(), sql=sql,
                                        experimental_python_types=self._experimental_pyton_types)
        query.execute()

    def _execute_prepared_statement(
        self,
        statement_name,
        params
    ):
        sql = 'EXECUTE ' + statement_name + ' USING ' + ','.join(map(self._format_prepared_param, params))
        return trino.client.TrinoQuery(self._request, sql=sql, experimental_python_types=self._experimental_pyton_types)

    def _format_prepared_param(self, param):
        """
        Formats parameters to be passed in an
        EXECUTE statement.
        """
        if param is None:
            return "NULL"

        if isinstance(param, bool):
            return "true" if param else "false"

        if isinstance(param, int):
            # TODO represent numbers exceeding 64-bit (BIGINT) as DECIMAL
            return "%d" % param

        if isinstance(param, float):
            if param == float("+inf"):
                return "infinity()"
            if param == float("-inf"):
                return "-infinity()"
            if math.isnan(param):
                return "nan()"
            return "DOUBLE '%s'" % param

        if isinstance(param, str):
            return ("'%s'" % param.replace("'", "''"))

        if isinstance(param, bytes):
            return "X'%s'" % param.hex()

        if isinstance(param, datetime.datetime) and param.tzinfo is None:
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
            return "TIMESTAMP '%s'" % datetime_str

        if isinstance(param, datetime.datetime) and param.tzinfo is not None:
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
            # named timezones
            if hasattr(param.tzinfo, 'zone'):
                return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.zone)
            # offset-based timezones
            return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.tzname(param))

        # We can't calculate the offset for a time without a point in time
        if isinstance(param, datetime.time) and param.tzinfo is None:
            time_str = param.strftime("%H:%M:%S.%f")
            return "TIME '%s'" % time_str

        if isinstance(param, datetime.date):
            date_str = param.strftime("%Y-%m-%d")
            return "DATE '%s'" % date_str

        if isinstance(param, list):
            return "ARRAY[%s]" % ','.join(map(self._format_prepared_param, param))

        if isinstance(param, tuple):
            return "ROW(%s)" % ','.join(map(self._format_prepared_param, param))

        if isinstance(param, dict):
            keys = list(param.keys())
            values = [param[key] for key in keys]
            return "MAP({}, {})".format(
                self._format_prepared_param(keys),
                self._format_prepared_param(values)
            )

        if isinstance(param, uuid.UUID):
            return "UUID '%s'" % param

        if isinstance(param, Decimal):
            return "DECIMAL '%s'" % param

        raise trino.exceptions.NotSupportedError("Query parameter of type '%s' is not supported." % type(param))

    def _deallocate_prepared_statement(self, statement_name: str) -> None:
        sql = 'DEALLOCATE PREPARE ' + statement_name
        query = trino.client.TrinoQuery(self.connection._create_request(), sql=sql,
                                        experimental_python_types=self._experimental_pyton_types)
        query.execute()

    def _generate_unique_statement_name(self):
        return 'st_' + uuid.uuid4().hex.replace('-', '')

    def execute(self, operation, params=None):
        if params:
            assert isinstance(params, (list, tuple)), (
                'params must be a list or tuple containing the query '
                'parameter values'
            )

            statement_name = self._generate_unique_statement_name()
            self._prepare_statement(operation, statement_name)

            try:
                # Send execute statement and assign the return value to `results`
                # as it will be returned by the function
                self._query = self._execute_prepared_statement(
                    statement_name, params
                )
                result = self._query.execute()
            finally:
                # Send deallocate statement
                # At this point the query can be deallocated since it has already
                # been executed
                # TODO: Consider caching prepared statements if requested by caller
                self._deallocate_prepared_statement(statement_name)

        else:
            self._query = trino.client.TrinoQuery(self._request, sql=operation,
                                                  experimental_python_types=self._experimental_pyton_types)
            result = self._query.execute()
        self._iterator = iter(result)
        return result

    def executemany(self, operation, seq_of_params):
        """
        PEP-0249: Prepare a database operation (query or command) and then
        execute it against all parameter sequences or mappings found in the sequence seq_of_parameters.
        Modules are free to implement this method using multiple calls to
        the .execute() method or by using array operations to have the
        database process the sequence as a whole in one call.

        Use of this method for an operation which produces one or more result
        sets constitutes undefined behavior, and the implementation is permitted (but not required)
        to raise an exception when it detects that a result set has been created by an invocation of the operation.

        The same comments as for .execute() also apply accordingly to this method.

        Return values are not defined.
        """
        for parameters in seq_of_params[:-1]:
            self.execute(operation, parameters)
            self.fetchall()
            if self._query.update_type is None:
                raise NotSupportedError("Query must return update type")
        if seq_of_params:
            self.execute(operation, seq_of_params[-1])
        else:
            self.execute(operation)

    def fetchone(self) -> Optional[List[Any]]:
        """

        PEP-0249: Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """

        try:
            assert self._iterator is not None
            return next(self._iterator)
        except StopIteration:
            return None
        except trino.exceptions.HttpError as err:
            raise trino.exceptions.OperationalError(str(err))

    def fetchmany(self, size=None) -> List[List[Any]]:
        """
        PEP-0249: Fetch the next set of rows of a query result, returning a
        sequence of sequences (e.g. a list of tuples). An empty sequence is
        returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If
        it is not given, the cursor's arraysize determines the number of rows
        to be fetched. The method should try to fetch as many rows as indicated
        by the size parameter. If this is not possible due to the specified
        number of rows not being available, fewer rows may be returned.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        Note there are performance considerations involved with the size
        parameter. For optimal performance, it is usually best to use the
        .arraysize attribute. If the size parameter is used, then it is best
        for it to retain the same value from one .fetchmany() call to the next.
        """

        if size is None:
            size = self.arraysize

        result = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def genall(self):
        return self._query.result

    def fetchall(self) -> List[List[Any]]:
        return list(self.genall())

    def cancel(self):
        if self._query is None:
            raise trino.exceptions.OperationalError(
                "Cancel query failed; no running query"
            )
        self._query.cancel()

    def close(self):
        self.cancel()
        # TODO: Cancel not only the last query executed on this cursor
        #  but also any other outstanding queries executed through this cursor.


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = datetime.date.fromtimestamp
TimestampFromTicks = datetime.datetime.fromtimestamp


def TimeFromTicks(ticks):
    return datetime.time(*datetime.localtime(ticks)[3:6])


def Binary(string):
    return string.encode("utf-8")


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = [v.lower() for v in values]

    def __eq__(self, other):
        return other.lower() in self.values


STRING = DBAPITypeObject("VARCHAR", "CHAR", "VARBINARY", "JSON", "IPADDRESS")

BINARY = DBAPITypeObject(
    "ARRAY", "MAP", "ROW", "HyperLogLog", "P4HyperLogLog", "QDigest"
)

NUMBER = DBAPITypeObject(
    "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "DECIMAL"
)

DATETIME = DBAPITypeObject(
    "DATE",
    "TIME",
    "TIME WITH TIME ZONE",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "INTERVAL YEAR TO MONTH",
    "INTERVAL DAY TO SECOND",
)

ROWID = DBAPITypeObject()  # nothing indicates row id in Trino
