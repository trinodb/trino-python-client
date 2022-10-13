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

This module implements the Trino protocol to submit SQL statements, track
their state and retrieve their result as described in
https://github.com/trinodb/trino/wiki/HTTP-Protocol
and Trino source code.

The outline of a query is:
- Send HTTP POST to the coordinator
- Retrieve HTTP response with ``nextUri``
- Get status of the query execution by sending a HTTP GET to the coordinator

Trino queries are managed by the ``TrinoQuery`` class. HTTP requests are
managed by the ``TrinoRequest`` class. the status of a query is represented
by ``TrinoStatus`` and the result by ``TrinoResult``.


The main interface is :class:`TrinoQuery`: ::

    >> request = TrinoRequest(host='coordinator', port=8080, user='test')
    >> query =  TrinoQuery(request, sql)
    >> rows = list(query.execute())
"""

import copy
import functools
import os
import random
import re
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

import pytz
import requests

import trino.logging
from trino import constants, exceptions

__all__ = ["ClientSession", "TrinoQuery", "TrinoRequest", "PROXIES"]

logger = trino.logging.get_logger(__name__)

MAX_ATTEMPTS = constants.DEFAULT_MAX_ATTEMPTS
SOCKS_PROXY = os.environ.get("SOCKS_PROXY")
if SOCKS_PROXY:
    PROXIES = {"http": "socks5://" + SOCKS_PROXY, "https": "socks5://" + SOCKS_PROXY}
else:
    PROXIES = {}

_HEADER_EXTRA_CREDENTIAL_KEY_REGEX = re.compile(r'^\S[^\s=]*$')

INF = float("inf")
NEGATIVE_INF = float("-inf")
NAN = float("nan")


class ClientSession(object):
    """
    Manage the current Client Session properties of a specific connection. This class is thread-safe.

    :param user: associated with the query. It is useful for access control
                 and query scheduling.
    :param source: associated with the query. It is useful for access
                   control and query scheduling.
    :param catalog: to query. The *catalog* is associated with a Trino
                    connector. This variable sets the default catalog used
                    by SQL statements. For example, if *catalog* is set
                    to ``some_catalog``, the SQL statement
                    ``SELECT * FROM some_schema.some_table`` will actually
                    query the table
                    ``some_catalog.some_schema.some_table``.
    :param schema: to query. The *schema* is a logical abstraction to group
                   table. This variable sets the default schema used by
                   SQL statements. For example, if *schema* is set to
                   ``some_schema``, the SQL statement
                   ``SELECT * FROM some_table`` will actually query the
                   table ``some_catalog.some_schema.some_table``.
    :param properties: set specific Trino behavior for the current
                               session. Please refer to the output of
                               ``SHOW SESSION`` to check the available
                               properties.
    :param headers: HTTP headers to POST/GET in the HTTP requests
    :param extra_credential: extra credentials. as list of ``(key, value)``
                             tuples.
    :param client_tags: Client tags as list of strings.
    :param roles: roles for the current session. Some connectors do not
                 support role management. See connector documentation for more details.
    """

    def __init__(
        self,
        user: str,
        catalog: str = None,
        schema: str = None,
        source: str = None,
        properties: Dict[str, str] = None,
        headers: Dict[str, str] = None,
        transaction_id: str = None,
        extra_credential: List[Tuple[str, str]] = None,
        client_tags: List[str] = None,
        roles: Dict[str, str] = None,
    ):
        self._user = user
        self._catalog = catalog
        self._schema = schema
        self._source = source
        self._properties = properties.copy() if properties is not None else {}
        self._headers = headers.copy() if headers is not None else {}
        self._transaction_id = transaction_id
        self._extra_credential = extra_credential
        self._client_tags = client_tags.copy() if client_tags is not None else list()
        self._roles = roles.copy() if roles is not None else {}
        self._prepared_statements: Dict[str, str] = {}
        self._object_lock = threading.Lock()

    @property
    def user(self):
        return self._user

    @property
    def catalog(self):
        with self._object_lock:
            return self._catalog

    @catalog.setter
    def catalog(self, catalog):
        with self._object_lock:
            self._catalog = catalog

    @property
    def schema(self):
        with self._object_lock:
            return self._schema

    @schema.setter
    def schema(self, schema):
        with self._object_lock:
            self._schema = schema

    @property
    def source(self):
        return self._source

    @property
    def properties(self):
        with self._object_lock:
            return self._properties

    @properties.setter
    def properties(self, properties):
        with self._object_lock:
            self._properties = properties

    @property
    def headers(self):
        return self._headers

    @property
    def transaction_id(self):
        with self._object_lock:
            return self._transaction_id

    @transaction_id.setter
    def transaction_id(self, transaction_id):
        with self._object_lock:
            self._transaction_id = transaction_id

    @property
    def extra_credential(self):
        return self._extra_credential

    @property
    def client_tags(self):
        return self._client_tags

    @property
    def roles(self):
        with self._object_lock:
            return self._roles

    @roles.setter
    def roles(self, roles):
        with self._object_lock:
            self._roles = roles

    @property
    def prepared_statements(self):
        return self._prepared_statements

    @prepared_statements.setter
    def prepared_statements(self, prepared_statements):
        with self._object_lock:
            self._prepared_statements = prepared_statements

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_object_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._object_lock = threading.Lock()


def get_header_values(headers, header):
    return [val.strip() for val in headers[header].split(",")]


def get_session_property_values(headers, header):
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), urllib.parse.unquote_plus(v.strip()))
        for k, v in (kv.split("=", 1) for kv in kvs if kv)
    ]


def get_prepared_statement_values(headers, header):
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), urllib.parse.unquote_plus(v.strip()))
        for k, v in (kv.split("=", 1) for kv in kvs if kv)
    ]


def get_roles_values(headers, header):
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), urllib.parse.unquote_plus(v.strip()))
        for k, v in (kv.split("=", 1) for kv in kvs if kv)
    ]


class TrinoStatus(object):
    def __init__(self, id, stats, warnings, info_uri, next_uri, update_type, rows, columns=None):
        self.id = id
        self.stats = stats
        self.warnings = warnings
        self.info_uri = info_uri
        self.next_uri = next_uri
        self.update_type = update_type
        self.rows = rows
        self.columns = columns

    def __repr__(self):
        return (
            "TrinoStatus("
            "id={}, stats={{...}}, warnings={}, info_uri={}, next_uri={}, rows=<count={}>"
            ")".format(
                self.id,
                len(self.warnings),
                self.info_uri,
                self.next_uri,
                len(self.rows),
            )
        )


class _DelayExponential(object):
    def __init__(
            self, base=0.1, exponent=2, jitter=True, max_delay=2 * 3600  # 100ms  # 2 hours
    ):
        self._base = base
        self._exponent = exponent
        self._jitter = jitter
        self._max_delay = max_delay

    def __call__(self, attempt):
        delay = float(self._base) * (self._exponent ** attempt)
        if self._jitter:
            delay *= random.random()
        delay = min(float(self._max_delay), delay)
        return delay


class _RetryWithExponentialBackoff(object):
    def __init__(
            self, base=0.1, exponent=2, jitter=True, max_delay=2 * 3600  # 100ms  # 2 hours
    ):
        self._get_delay = _DelayExponential(base, exponent, jitter, max_delay)

    def retry(self, func, args, kwargs, err, attempt):
        delay = self._get_delay(attempt)
        time.sleep(delay)


class TrinoRequest(object):
    """
    Manage the HTTP requests of a Trino query.

    :param host: name of the coordinator
    :param port: TCP port to connect to the coordinator
    :param http_scheme: "http" or "https"
    :param auth: class that manages user authentication. ``None`` means no
                 authentication.
    :max_attempts: maximum number of attempts when sending HTTP requests. An
                   attempt is an HTTP request. 5 attempts means 4 retries.
    :request_timeout: How long (in seconds) to wait for the server to send
                      data before giving up, as a float or a
                      ``(connect timeout, read timeout)`` tuple.

    The client initiates a query by sending an HTTP POST to the
    coordinator. It then gets a response back from the coordinator with:
    - An URI to query to get the status for the query and the remaining
      data
    - An URI to get more information about the execution of the query
    - Statistics about the current query execution

    Please refer to :class:`TrinoStatus` to access the status returned by
    :meth:`TrinoRequest.process`.

    When the client makes an HTTP request, it may encounter the following
    errors:
    - Connection or read timeout:
      - There is a network partition and TCP segments are
        either dropped or delayed.
      - The coordinator stalled because of an OS level stall (page allocation
        stall, long time to page in pages, etc...), a JVM stall (full GC), or
        an application level stall (thread starving, lock contention)
    - Connection refused: Configuration or runtime issue on the coordinator
    - Connection closed:

    As most of these errors are transient, the question the caller should set
    retries with respect to when they want to notify the application that uses
    the client.
    """

    http = requests

    HTTP_EXCEPTIONS = (
        http.ConnectionError,
        http.Timeout,
    )

    def __init__(
        self,
        host: str,
        port: int,
        client_session: ClientSession,
        http_session: Any = None,
        http_scheme: str = None,
        auth: Optional[Any] = constants.DEFAULT_AUTH,
        redirect_handler: Any = None,
        max_attempts: int = MAX_ATTEMPTS,
        request_timeout: Union[float, Tuple[float, float]] = constants.DEFAULT_REQUEST_TIMEOUT,
        handle_retry=_RetryWithExponentialBackoff(),
        verify: bool = True,
    ) -> None:
        self._client_session = client_session
        self._host = host
        self._port = port
        self._next_uri: Optional[str] = None

        if http_scheme is None:
            if self._port == constants.DEFAULT_TLS_PORT:
                self._http_scheme = constants.HTTPS
            else:
                self._http_scheme = constants.HTTP
        else:
            self._http_scheme = http_scheme

        if http_session is not None:
            self._http_session = http_session
        else:
            self._http_session = self.http.Session()
            self._http_session.verify = verify
        self._http_session.headers.update(self.http_headers)
        self._exceptions = self.HTTP_EXCEPTIONS
        self._auth = auth
        if self._auth:
            if self._http_scheme == constants.HTTP:
                raise ValueError("cannot use authentication with HTTP")
            self._auth.set_http_session(self._http_session)
            self._exceptions += self._auth.get_exceptions()

        self._redirect_handler = redirect_handler
        self._request_timeout = request_timeout
        self._handle_retry = handle_retry
        self.max_attempts = max_attempts

    @property
    def transaction_id(self):
        return self._client_session.transaction_id

    @transaction_id.setter
    def transaction_id(self, value):
        self._client_session.transaction_id = value

    @property
    def http_headers(self) -> Dict[str, str]:
        headers = {}

        headers[constants.HEADER_CATALOG] = self._client_session.catalog
        headers[constants.HEADER_SCHEMA] = self._client_session.schema
        headers[constants.HEADER_SOURCE] = self._client_session.source
        headers[constants.HEADER_USER] = self._client_session.user
        if len(self._client_session.roles.values()):
            headers[constants.HEADER_ROLE] = ",".join(
                # ``name`` must not contain ``=``
                "{}={}".format(catalog, urllib.parse.quote(str(role)))
                for catalog, role in self._client_session.roles.items()
            )
        if self._client_session.client_tags is not None and len(self._client_session.client_tags) > 0:
            headers[constants.HEADER_CLIENT_TAGS] = ",".join(self._client_session.client_tags)

        headers[constants.HEADER_SESSION] = ",".join(
            # ``name`` must not contain ``=``
            "{}={}".format(name, urllib.parse.quote(str(value)))
            for name, value in self._client_session.properties.items()
        )

        if len(self._client_session.prepared_statements) != 0:
            # ``name`` must not contain ``=``
            headers[constants.HEADER_PREPARED_STATEMENT] = ",".join(
                "{}={}".format(name, urllib.parse.quote_plus(statement))
                for name, statement in self._client_session.prepared_statements.items()
            )

        # merge custom http headers
        for key in self._client_session.headers:
            if key in headers.keys():
                raise ValueError("cannot override reserved HTTP header {}".format(key))
        headers.update(self._client_session.headers)

        transaction_id = self._client_session.transaction_id
        headers[constants.HEADER_TRANSACTION] = transaction_id

        if self._client_session.extra_credential is not None and \
                len(self._client_session.extra_credential) > 0:

            for tup in self._client_session.extra_credential:
                self._verify_extra_credential(tup)

            # HTTP 1.1 section 4.2 combine multiple extra credentials into a
            # comma-separated value
            # extra credential value is encoded per spec (application/x-www-form-urlencoded MIME format)
            headers[constants.HEADER_EXTRA_CREDENTIAL] = \
                ", ".join(
                    [f"{tup[0]}={urllib.parse.quote_plus(tup[1])}" for tup in self._client_session.extra_credential])

        return headers

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    @max_attempts.setter
    def max_attempts(self, value) -> None:
        self._max_attempts = value
        if value == 1:  # No retry
            self._get = self._http_session.get
            self._post = self._http_session.post
            self._delete = self._http_session.delete
            return

        with_retry = _retry_with(
            self._handle_retry,
            handled_exceptions=self._exceptions,
            conditions=(
                # need retry when there is no exception but the status code is 502, 503, or 504
                lambda response: getattr(response, "status_code", None)
                in (502, 503, 504),
            ),
            max_attempts=self._max_attempts,
        )
        self._get = with_retry(self._http_session.get)
        self._post = with_retry(self._http_session.post)
        self._delete = with_retry(self._http_session.delete)

    def get_url(self, path) -> str:
        return "{protocol}://{host}:{port}{path}".format(
            protocol=self._http_scheme, host=self._host, port=self._port, path=path
        )

    @property
    def statement_url(self) -> str:
        return self.get_url(constants.URL_STATEMENT_PATH)

    @property
    def next_uri(self) -> Optional[str]:
        return self._next_uri

    def post(self, sql, additional_http_headers=None):
        data = sql.encode("utf-8")
        # Deep copy of the http_headers dict since they may be modified for this
        # request by the provided additional_http_headers
        http_headers = copy.deepcopy(self.http_headers)

        # Update the request headers with the additional_http_headers
        http_headers.update(additional_http_headers or {})

        http_response = self._post(
            self.statement_url,
            data=data,
            headers=http_headers,
            timeout=self._request_timeout,
            allow_redirects=self._redirect_handler is None,
            proxies=PROXIES,
        )
        if self._redirect_handler is not None:
            while http_response is not None and http_response.is_redirect:
                location = http_response.headers["Location"]
                url = self._redirect_handler.handle(location)
                logger.info("redirect %s from %s to %s", http_response.status_code, location, url)
                http_response = self._post(
                    url,
                    data=data,
                    headers=http_headers,
                    timeout=self._request_timeout,
                    allow_redirects=False,
                    proxies=PROXIES,
                )
        return http_response

    def get(self, url):
        return self._get(
            url,
            headers=self.http_headers,
            timeout=self._request_timeout,
            proxies=PROXIES,
        )

    def delete(self, url):
        return self._delete(url, timeout=self._request_timeout, proxies=PROXIES)

    def _process_error(self, error, query_id):
        error_type = error["errorType"]
        if error_type == "EXTERNAL":
            raise exceptions.TrinoExternalError(error, query_id)
        elif error_type == "USER_ERROR":
            return exceptions.TrinoUserError(error, query_id)

        return exceptions.TrinoQueryError(error, query_id)

    def raise_response_error(self, http_response):
        if http_response.status_code == 502:
            raise exceptions.Http502Error("error 502: bad gateway")

        if http_response.status_code == 503:
            raise exceptions.Http503Error("error 503: service unavailable")

        if http_response.status_code == 504:
            raise exceptions.Http504Error("error 504: gateway timeout")

        raise exceptions.HttpError(
            "error {}{}".format(
                http_response.status_code,
                ": {}".format(http_response.content) if http_response.content else "",
            )
        )

    def process(self, http_response) -> TrinoStatus:
        if not http_response.ok:
            self.raise_response_error(http_response)

        http_response.encoding = "utf-8"
        response = http_response.json()
        logger.debug("HTTP %s: %s", http_response.status_code, response)
        if "error" in response:
            raise self._process_error(response["error"], response.get("id"))

        if constants.HEADER_CLEAR_SESSION in http_response.headers:
            for prop in get_header_values(
                http_response.headers, constants.HEADER_CLEAR_SESSION
            ):
                self._client_session.properties.pop(prop, None)

        if constants.HEADER_SET_SESSION in http_response.headers:
            for key, value in get_session_property_values(
                http_response.headers, constants.HEADER_SET_SESSION
            ):
                self._client_session.properties[key] = value

        if constants.HEADER_SET_CATALOG in http_response.headers:
            self._client_session.catalog = http_response.headers[constants.HEADER_SET_CATALOG]

        if constants.HEADER_SET_SCHEMA in http_response.headers:
            self._client_session.schema = http_response.headers[constants.HEADER_SET_SCHEMA]

        if constants.HEADER_SET_ROLE in http_response.headers:
            for key, value in get_roles_values(
                    http_response.headers, constants.HEADER_SET_ROLE
            ):
                self._client_session.roles[key] = value

        if constants.HEADER_ADDED_PREPARE in http_response.headers:
            for name, statement in get_prepared_statement_values(
                http_response.headers, constants.HEADER_ADDED_PREPARE
            ):
                self._client_session.prepared_statements[name] = statement

        if constants.HEADER_DEALLOCATED_PREPARE in http_response.headers:
            for name in get_header_values(
                http_response.headers, constants.HEADER_DEALLOCATED_PREPARE
            ):
                self._client_session.prepared_statements.pop(name, None)

        self._next_uri = response.get("nextUri")

        return TrinoStatus(
            id=response["id"],
            stats=response["stats"],
            warnings=response.get("warnings", []),
            info_uri=response["infoUri"],
            next_uri=self._next_uri,
            update_type=response.get("updateType"),
            rows=response.get("data", []),
            columns=response.get("columns"),
        )

    def _verify_extra_credential(self, header):
        """
        Verifies that key has ASCII only and non-whitespace characters.
        """
        key = header[0]

        if not _HEADER_EXTRA_CREDENTIAL_KEY_REGEX.match(key):
            raise ValueError(f"whitespace or '=' are disallowed in extra credential '{key}'")

        try:
            key.encode().decode('ascii')
        except UnicodeDecodeError:
            raise ValueError(f"only ASCII characters are allowed in extra credential '{key}'")


class TrinoResult(object):
    """
    Represent the result of a Trino query as an iterator on rows.

    This class implements the iterator protocol as a generator type
    https://docs.python.org/3/library/stdtypes.html#generator-types
    """

    def __init__(self, query, rows: List[Any]):
        self._query = query
        # Initial rows from the first POST request
        self._rows = rows
        self._rownumber = 0

    @property
    def rows(self):
        return self._rows

    @rows.setter
    def rows(self, rows):
        self._rows = rows

    @property
    def rownumber(self) -> int:
        return self._rownumber

    def __iter__(self):
        # A query only transitions to a FINISHED state when the results are fully consumed:
        # The reception of the data is acknowledged by calling the next_uri before exposing the data through dbapi.
        while not self._query.finished or self._rows is not None:
            next_rows = self._query.fetch() if not self._query.finished else None
            for row in self._rows:
                self._rownumber += 1
                logger.debug("row %s", row)
                yield row

            self._rows = next_rows


class TrinoQuery(object):
    """Represent the execution of a SQL statement by Trino."""

    def __init__(
            self,
            request: TrinoRequest,
            sql: str,
            experimental_python_types: bool = False,
    ) -> None:
        self.query_id: Optional[str] = None

        self._stats: Dict[Any, Any] = {}
        self._info_uri: Optional[str] = None
        self._warnings: List[Dict[Any, Any]] = []
        self._columns: Optional[List[str]] = None
        self._finished = False
        self._cancelled = False
        self._request = request
        self._update_type = None
        self._sql = sql
        self._result: Optional[TrinoResult] = None
        self._experimental_python_types = experimental_python_types
        self._row_mapper: Optional[RowMapper] = None

    @property
    def columns(self):
        if self.query_id:
            while not self._columns and not self.finished and not self.cancelled:
                # Columns are not returned immediately after query is submitted.
                # Continue fetching data until columns information is available and push fetched rows into buffer.
                self._result.rows += self.fetch()
        return self._columns

    @property
    def stats(self):
        return self._stats

    @property
    def update_type(self):
        return self._update_type

    @property
    def warnings(self):
        return self._warnings

    @property
    def result(self):
        return self._result

    @property
    def info_uri(self):
        return self._info_uri

    def execute(self, additional_http_headers=None) -> TrinoResult:
        """Initiate a Trino query by sending the SQL statement

        This is the first HTTP request sent to the coordinator.
        It sets the query_id and returns a Result object used to
        track the rows returned by the query. To fetch all rows,
        call fetch() until finished is true.
        """
        if self.cancelled:
            raise exceptions.TrinoUserError("Query has been cancelled", self.query_id)

        response = self._request.post(self._sql, additional_http_headers)
        status = self._request.process(response)
        self._info_uri = status.info_uri
        self.query_id = status.id
        self._stats.update({"queryId": self.query_id})
        self._update_state(status)
        self._warnings = getattr(status, "warnings", [])
        if status.next_uri is None:
            self._finished = True

        rows = self._row_mapper.map(status.rows) if self._row_mapper else status.rows
        self._result = TrinoResult(self, rows)

        # Execute should block until at least one row is received or query is finished or cancelled
        while not self.finished and not self.cancelled and len(self._result.rows) == 0:
            self._result.rows += self.fetch()
        return self._result

    def _update_state(self, status):
        self._stats.update(status.stats)
        self._update_type = status.update_type
        if not self._row_mapper and status.columns:
            self._row_mapper = RowMapperFactory().create(columns=status.columns,
                                                         experimental_python_types=self._experimental_python_types)
        if status.columns:
            self._columns = status.columns

    def fetch(self) -> List[List[Any]]:
        """Continue fetching data for the current query_id"""
        response = self._request.get(self._request.next_uri)
        status = self._request.process(response)
        self._update_state(status)
        logger.debug(status)
        if status.next_uri is None:
            self._finished = True

        if not self._row_mapper:
            return []

        return self._row_mapper.map(status.rows)

    def cancel(self) -> None:
        """Cancel the current query"""
        if self.query_id is None or self.finished:
            return

        url = self._request.get_url("/v1/query/{}".format(self.query_id))
        logger.debug("cancelling query: %s", self.query_id)
        response = self._request.delete(url)
        logger.debug(response)
        if response.status_code == requests.codes.no_content:
            self._cancelled = True
            logger.debug("query cancelled: %s", self.query_id)
            return

        self._request.raise_response_error(response)

    def is_finished(self) -> bool:
        import warnings
        warnings.warn("is_finished is deprecated, use finished instead", DeprecationWarning)
        return self.finished

    @property
    def finished(self) -> bool:
        return self._finished

    @property
    def cancelled(self) -> bool:
        return self._cancelled


def _retry_with(handle_retry, handled_exceptions, conditions, max_attempts):
    def wrapper(func):
        @functools.wraps(func)
        def decorated(*args, **kwargs):
            error = None
            result = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if any(guard(result) for guard in conditions):
                        handle_retry.retry(func, args, kwargs, None, attempt)
                        continue
                    return result
                except Exception as err:
                    error = err
                    if any(isinstance(err, exc) for exc in handled_exceptions):
                        handle_retry.retry(func, args, kwargs, err, attempt)
                        continue
                    break
            logger.info("failed after %s attempts", attempt)
            if error is not None:
                raise error
            return result

        return decorated

    return wrapper


class NoOpRowMapper:
    """
    No-op RowMapper which does not perform any transformation
    Used when experimental_python_types is False.
    """

    def map(self, rows):
        return rows


class RowMapperFactory:
    """
    Given the 'columns' result from Trino, generate a list of
    lambda functions (one for each column) which will process a data value
    and returns a RowMapper instance which will process rows of data
    """
    no_op_row_mapper = NoOpRowMapper()

    def create(self, columns, experimental_python_types):
        assert columns is not None

        if experimental_python_types:
            return RowMapper([self._col_func(column['typeSignature']) for column in columns])
        return RowMapperFactory.no_op_row_mapper

    def _col_func(self, column):
        col_type = column['rawType']

        if col_type == 'array':
            return self._array_map_func(column)
        elif col_type == 'row':
            return self._row_map_func(column)
        elif col_type == 'map':
            return self._map_map_func(column)
        elif col_type.startswith('decimal'):
            return lambda val: Decimal(val)
        elif col_type.startswith('double') or col_type.startswith('real'):
            return self._double_map_func()
        elif col_type.startswith('timestamp'):
            return self._timestamp_map_func(column, col_type)
        elif col_type.startswith('time'):
            return self._time_map_func(column, col_type)
        elif col_type == 'date':
            return lambda val: datetime.strptime(val, '%Y-%m-%d').date()
        else:
            return lambda val: val

    def _array_map_func(self, column):
        element_mapping_func = self._col_func(column['arguments'][0]['value'])
        return lambda values: [element_mapping_func(value) for value in values]

    def _row_map_func(self, column):
        element_mapping_func = [self._col_func(arg['value']['typeSignature']) for arg in column['arguments']]
        return lambda values: tuple(element_mapping_func[idx](value) for idx, value in enumerate(values))

    def _map_map_func(self, column):
        key_mapping_func = self._col_func(column['arguments'][0]['value'])
        value_mapping_func = self._col_func(column['arguments'][1]['value'])
        return lambda values: {key_mapping_func(key): value_mapping_func(value) for key, value in values.items()}

    def _double_map_func(self):
        return lambda val: INF if val == 'Infinity' \
            else NEGATIVE_INF if val == '-Infinity' \
            else NAN if val == 'NaN' \
            else float(val)

    def _timestamp_map_func(self, column, col_type):
        datetime_default_size = 20  # size of 'YYYY-MM-DD HH:MM:SS.' (the datetime string up to the milliseconds)
        pattern = "%Y-%m-%d %H:%M:%S"
        ms_size, ms_to_trim = self._get_number_of_digits(column)
        if ms_size > 0:
            pattern += ".%f"

        dt_size = datetime_default_size + ms_size - ms_to_trim
        dt_tz_offset = datetime_default_size + ms_size
        if 'with time zone' in col_type:

            if ms_to_trim > 0:
                return lambda val: \
                    [datetime.strptime(val[:dt_size] + val[dt_tz_offset:], pattern + ' %z')
                        if tz.startswith('+') or tz.startswith('-')
                        else datetime.strptime(dt[:dt_size] + dt[dt_tz_offset:], pattern)
                                     .replace(tzinfo=pytz.timezone(tz))
                        for dt, tz in [val.rsplit(' ', 1)]][0]
            else:
                return lambda val: [datetime.strptime(val, pattern + ' %z')
                                    if tz.startswith('+') or tz.startswith('-')
                                    else datetime.strptime(dt, pattern).replace(tzinfo=pytz.timezone(tz))
                                    for dt, tz in [val.rsplit(' ', 1)]][0]

        if ms_to_trim > 0:
            return lambda val: datetime.strptime(val[:dt_size] + val[dt_tz_offset:], pattern)
        else:
            return lambda val: datetime.strptime(val, pattern)

    def _time_map_func(self, column, col_type):
        pattern = "%H:%M:%S"
        ms_size, ms_to_trim = self._get_number_of_digits(column)
        if ms_size > 0:
            pattern += ".%f"

        time_size = 9 + ms_size - ms_to_trim

        if 'with time zone' in col_type:
            return lambda val: self._get_time_with_timezome(val, time_size, pattern)
        else:
            return lambda val: datetime.strptime(val[:time_size], pattern).time()

    def _get_time_with_timezome(self, value, time_size, pattern):
        matches = re.match(r'^(.*)([\+\-])(\d{2}):(\d{2})$', value)
        assert matches is not None
        assert len(matches.groups()) == 4
        if matches.group(2) == '-':
            tz = -timedelta(hours=int(matches.group(3)), minutes=int(matches.group(4)))
        else:
            tz = timedelta(hours=int(matches.group(3)), minutes=int(matches.group(4)))
        return datetime.strptime(matches.group(1)[:time_size], pattern).time().replace(tzinfo=timezone(tz))

    def _get_number_of_digits(self, column):
        args = column['arguments']
        if len(args) == 0:
            return 3, 0
        ms_size = column['arguments'][0]['value']
        if ms_size == 0:
            return -1, 0
        ms_to_trim = ms_size - min(ms_size, 6)
        return ms_size, ms_to_trim


class RowMapper:
    """
    Maps a row of data given a list of mapping functions
    """
    def __init__(self, columns):
        self.columns = columns

    def map(self, rows):
        if len(self.columns) == 0:
            return rows
        return [self._map_row(row) for row in rows]

    def _map_row(self, row):
        return [self._map_value(value, self.columns[idx]) for idx, value in enumerate(row)]

    def _map_value(self, value, col_mapping_func):
        if value is None:
            return None

        try:
            return col_mapping_func(value)
        except ValueError as e:
            error_str = f"Could not convert '{value}' into the associated python type"
            raise trino.exceptions.TrinoDataError(error_str) from e
