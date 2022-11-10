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
from __future__ import annotations

import abc
import copy
import functools
import os
import random
import re
import threading
import urllib.parse
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
from time import sleep
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar, Union

import pytz
import requests
from pytz.tzinfo import BaseTzInfo

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

T = TypeVar("T")

PythonTemporalType = TypeVar("PythonTemporalType", bound=Union[time, datetime])
POWERS_OF_TEN: Dict[int, Decimal] = {}
for i in range(0, 13):
    POWERS_OF_TEN[i] = Decimal(10 ** i)
MAX_PYTHON_TEMPORAL_PRECISION = POWERS_OF_TEN[6]


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
        sleep(delay)


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

    def post(self, sql: str, additional_http_headers: Optional[Dict[str, Any]] = None):
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

    def get(self, url: str):
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


class ValueMapper(abc.ABC, Generic[T]):
    @abc.abstractmethod
    def map(self, value: Any) -> Optional[T]:
        pass


class NoOpValueMapper(ValueMapper[Any]):
    def map(self, value) -> Optional[Any]:
        return value


class DecimalValueMapper(ValueMapper[Decimal]):
    def map(self, value) -> Optional[Decimal]:
        if value is None:
            return None
        return Decimal(value)


class DoubleValueMapper(ValueMapper[float]):
    def map(self, value) -> Optional[float]:
        if value is None:
            return None
        if value == 'Infinity':
            return float("inf")
        if value == '-Infinity':
            return float("-inf")
        if value == 'NaN':
            return float("nan")
        return float(value)


def _create_tzinfo(timezone_str: str) -> tzinfo:
    if timezone_str.startswith("+") or timezone_str.startswith("-"):
        hours = timezone_str[1:3]
        minutes = timezone_str[4:6]
        if timezone_str.startswith("-"):
            return timezone(-timedelta(hours=int(hours), minutes=int(minutes)))
        return timezone(timedelta(hours=int(hours), minutes=int(minutes)))
    else:
        return pytz.timezone(timezone_str)


def _fraction_to_decimal(fractional_str: str) -> Decimal:
    return Decimal(fractional_str or 0) / POWERS_OF_TEN[len(fractional_str)]


class TemporalType(Generic[PythonTemporalType], metaclass=abc.ABCMeta):
    def __init__(self, whole_python_temporal_value: PythonTemporalType, remaining_fractional_seconds: Decimal):
        self._whole_python_temporal_value = whole_python_temporal_value
        self._remaining_fractional_seconds = remaining_fractional_seconds

    @abc.abstractmethod
    def new_instance(self, value: PythonTemporalType, fraction: Decimal) -> TemporalType[PythonTemporalType]:
        pass

    @abc.abstractmethod
    def to_python_type(self) -> PythonTemporalType:
        pass

    def round_to(self, precision: int) -> TemporalType:
        """
            Python datetime and time only support up to microsecond precision
            In case the supplied value exceeds the specified precision,
            the value needs to be rounded.
        """
        remaining_fractional_seconds = self._remaining_fractional_seconds
        digits = abs(remaining_fractional_seconds.as_tuple().exponent)
        if digits > precision:
            rounding_factor = POWERS_OF_TEN[precision]
            rounded = remaining_fractional_seconds.quantize(Decimal(1 / rounding_factor))
            if rounded == rounding_factor:
                return self.new_instance(
                    self.normalize(self.add_time_delta(timedelta(seconds=1))),
                    Decimal(0)
                )
            return self.new_instance(self._whole_python_temporal_value, rounded)
        return self

    @abc.abstractmethod
    def add_time_delta(self, time_delta: timedelta) -> PythonTemporalType:
        """
            This method shall be overriden to implement fraction arithmetics.
        """
        pass

    def normalize(self, value: PythonTemporalType) -> PythonTemporalType:
        """
            If `add_time_delta` results in value crossing DST boundaries, this method should
            return a normalized version of the value to account for it, for example,
            using `pytz.timezone.normalize`.
        """
        return value


class Time(TemporalType[time]):
    def new_instance(self, value: time, fraction: Decimal) -> TemporalType[time]:
        return Time(value, fraction)

    def to_python_type(self) -> time:
        if self._remaining_fractional_seconds > 0:
            time_delta = timedelta(microseconds=int(self._remaining_fractional_seconds * MAX_PYTHON_TEMPORAL_PRECISION))
            return self.add_time_delta(time_delta)
        return self._whole_python_temporal_value

    def add_time_delta(self, time_delta: timedelta) -> time:
        time_delta_added = datetime.combine(datetime(1, 1, 1), self._whole_python_temporal_value) + time_delta
        return time_delta_added.time().replace(tzinfo=self._whole_python_temporal_value.tzinfo)


class TimeWithTimeZone(Time, TemporalType[time]):
    def new_instance(self, value: time, fraction: Decimal) -> TemporalType[time]:
        return TimeWithTimeZone(value, fraction)


class Timestamp(TemporalType[datetime]):
    def new_instance(self, value: datetime, fraction: Decimal) -> Timestamp:
        return Timestamp(value, fraction)

    def to_python_type(self) -> datetime:
        if self._remaining_fractional_seconds > 0:
            time_delta = timedelta(microseconds=int(self._remaining_fractional_seconds * MAX_PYTHON_TEMPORAL_PRECISION))
            return self.add_time_delta(time_delta)
        return self._whole_python_temporal_value

    def add_time_delta(self, time_delta: timedelta) -> datetime:
        return self._whole_python_temporal_value + time_delta


class TimestampWithTimeZone(Timestamp, TemporalType[datetime]):
    def new_instance(self, value: datetime, fraction: Decimal) -> TimestampWithTimeZone:
        return TimestampWithTimeZone(value, fraction)

    def normalize(self, value: datetime) -> datetime:
        if isinstance(self._whole_python_temporal_value.tzinfo, BaseTzInfo):
            return self._whole_python_temporal_value.tzinfo.normalize(value)
        return value


class TimeValueMapper(ValueMapper[time]):
    def __init__(self, precision):
        self.time_default_size = 8  # size of 'HH:MM:SS'
        self.precision = precision

    def map(self, value) -> Optional[time]:
        if value is None:
            return None
        whole_python_temporal_value = value[:self.time_default_size]
        remaining_fractional_seconds = value[self.time_default_size + 1:]
        return Time(
            time.fromisoformat(whole_python_temporal_value),
            _fraction_to_decimal(remaining_fractional_seconds)
        ).round_to(self.precision).to_python_type()

    def _add_second(self, time_value: time) -> time:
        return (datetime.combine(datetime(1, 1, 1), time_value) + timedelta(seconds=1)).time()


class TimeWithTimeZoneValueMapper(TimeValueMapper):
    def map(self, value) -> Optional[time]:
        if value is None:
            return None
        whole_python_temporal_value = value[:self.time_default_size]
        remaining_fractional_seconds = value[self.time_default_size + 1:len(value) - 6]
        timezone_part = value[len(value) - 6:]
        return TimeWithTimeZone(
            time.fromisoformat(whole_python_temporal_value).replace(tzinfo=_create_tzinfo(timezone_part)),
            _fraction_to_decimal(remaining_fractional_seconds),
        ).round_to(self.precision).to_python_type()


class DateValueMapper(ValueMapper[date]):
    def map(self, value) -> Optional[date]:
        if value is None:
            return None
        return date.fromisoformat(value)


class TimestampValueMapper(ValueMapper[datetime]):
    def __init__(self, precision):
        self.datetime_default_size = 19  # size of 'YYYY-MM-DD HH:MM:SS' (the datetime string up to the seconds)
        self.precision = precision

    def map(self, value) -> Optional[datetime]:
        if value is None:
            return None
        whole_python_temporal_value = value[:self.datetime_default_size]
        remaining_fractional_seconds = value[self.datetime_default_size + 1:]
        return Timestamp(
            datetime.fromisoformat(whole_python_temporal_value),
            _fraction_to_decimal(remaining_fractional_seconds),
        ).round_to(self.precision).to_python_type()


class TimestampWithTimeZoneValueMapper(TimestampValueMapper):
    def map(self, value) -> Optional[datetime]:
        if value is None:
            return None
        datetime_with_fraction, timezone_part = value.rsplit(' ', 1)
        whole_python_temporal_value = datetime_with_fraction[:self.datetime_default_size]
        remaining_fractional_seconds = datetime_with_fraction[self.datetime_default_size + 1:]
        return TimestampWithTimeZone(
            datetime.fromisoformat(whole_python_temporal_value).replace(tzinfo=_create_tzinfo(timezone_part)),
            _fraction_to_decimal(remaining_fractional_seconds),
        ).round_to(self.precision).to_python_type()


class ArrayValueMapper(ValueMapper[List[Optional[Any]]]):
    def __init__(self, mapper: ValueMapper[Any]):
        self.mapper = mapper

    def map(self, values: List[Any]) -> Optional[List[Any]]:
        if values is None:
            return None
        return [self.mapper.map(value) for value in values]


class RowValueMapper(ValueMapper[Tuple[Optional[Any], ...]]):
    def __init__(self, mappers: List[ValueMapper[Any]]):
        self.mappers = mappers

    def map(self, values: List[Any]) -> Optional[Tuple[Optional[Any], ...]]:
        if values is None:
            return None
        return tuple(self.mappers[index].map(value) for index, value in enumerate(values))


class MapValueMapper(ValueMapper[Dict[Any, Optional[Any]]]):
    def __init__(self, key_mapper: ValueMapper[Any], value_mapper: ValueMapper[Any]):
        self.key_mapper = key_mapper
        self.value_mapper = value_mapper

    def map(self, values: Any) -> Optional[Dict[Any, Optional[Any]]]:
        if values is None:
            return None
        return {
            self.key_mapper.map(key): self.value_mapper.map(value) for key, value in values.items()
        }


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
    NO_OP_ROW_MAPPER = NoOpRowMapper()

    def create(self, columns, experimental_python_types):
        assert columns is not None

        if experimental_python_types:
            return RowMapper([self._create_value_mapper(column['typeSignature']) for column in columns])
        return RowMapperFactory.NO_OP_ROW_MAPPER

    def _create_value_mapper(self, column) -> ValueMapper:
        col_type = column['rawType']

        if col_type == 'array':
            value_mapper = self._create_value_mapper(column['arguments'][0]['value'])
            return ArrayValueMapper(value_mapper)
        elif col_type == 'row':
            mappers = [self._create_value_mapper(arg['value']['typeSignature']) for arg in column['arguments']]
            return RowValueMapper(mappers)
        elif col_type == 'map':
            key_mapper = self._create_value_mapper(column['arguments'][0]['value'])
            value_mapper = self._create_value_mapper(column['arguments'][1]['value'])
            return MapValueMapper(key_mapper, value_mapper)
        elif col_type.startswith('decimal'):
            return DecimalValueMapper()
        elif col_type.startswith('double') or col_type.startswith('real'):
            return DoubleValueMapper()
        elif col_type.startswith('timestamp') and 'with time zone' in col_type:
            return TimestampWithTimeZoneValueMapper(self._get_precision(column))
        elif col_type.startswith('timestamp'):
            return TimestampValueMapper(self._get_precision(column))
        elif col_type.startswith('time') and 'with time zone' in col_type:
            return TimeWithTimeZoneValueMapper(self._get_precision(column))
        elif col_type.startswith('time'):
            return TimeValueMapper(self._get_precision(column))
        elif col_type == 'date':
            return DateValueMapper()
        else:
            return NoOpValueMapper()

    def _get_precision(self, column: Dict[str, Any]):
        args = column['arguments']
        if len(args) == 0:
            return 3
        return args[0]['value']


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
        return [self._map_value(value, self.columns[index]) for index, value in enumerate(row)]

    def _map_value(self, value, value_mapper: ValueMapper[T]) -> Optional[T]:
        try:
            return value_mapper.map(value)
        except ValueError as e:
            error_str = f"Could not convert '{value}' into the associated python type"
            raise trino.exceptions.TrinoDataError(error_str) from e
