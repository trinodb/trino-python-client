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

This module implements the Presto protocol to submit SQL statements, track
their state and retrieve their result as described in
https://github.com/prestodb/presto/wiki/HTTP-Protocol
and Presto source code.

The outline of a query is:
- Send HTTP POST to the coordinator
- Retrieve HTTP response with ``nextUri``
- Get status of the query execution by sending a HTTP GET to the coordinator

Presto queries are managed by the ``PrestoQuery`` class. HTTP requests are
managed by the ``PrestoRequest`` class. the status of a query is represented
by ``PrestoStatus`` and the result by ``PrestoResult``.


The main interface is :class:`PrestoQuery`: ::

    >> request = PrestoRequest(host='coordinator', port=8080, user='test')
    >> query =  PrestoQuery(request, sql)
    >> rows = list(query.execute())
"""

from urllib.parse import urlunparse
import logging
from typing import Any, Dict, List, Optional, Text  # NOQA for mypy types

import requests

from prestodb import constants
from prestodb import exceptions


__all__ = ['PrestoQuery', 'PrestoRequest']


logger = logging.getLogger(__name__)


MAX_ATTEMPTS = constants.DEFAULT_MAX_ATTEMPTS


class ClientSession(object):
    def __init__(
        self,
        catalog,
        schema,
        source,
        user,
        properties=None,
        headers=None,
    ):
        self.catalog = catalog
        self.schema = schema
        self.source = source
        self.user = user
        if properties is None:
            properties = {}
        self._properties = properties
        self._headers = headers or {}

    @property
    def properties(self):
        return self._properties

    @property
    def headers(self):
        return self._headers


def get_header_values(headers, header):
    return [val.strip() for val in headers[header].split(',')]


def get_session_property_values(headers, header):
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), v.strip()) for k, v
        in (kv.split('=', 1) for kv in kvs)
    ]


class PrestoStatus(object):
    def __init__(self, id, stats, info_uri, next_uri, rows, columns=None):
        self.id = id
        self.stats = stats
        self.info_uri = info_uri
        self.next_uri = next_uri
        self.rows = rows
        self.columns = columns

    def __repr__(self):
        return (
            'PrestoStatus('
            'id={}, stats={{...}}, info_uri={}, next_uri={}, rows=<count={}>'
            ')'.format(
                self.id,
                self.info_uri,
                self.next_uri,
                len(self.rows),
            )
        )


class PrestoRequest(object):
    """
    Manage the HTTP requests of a Presto query.

    :param host: name of the coordinator
    :param port: TCP port to connect to the coordinator
    :param user: associated with the query. It is useful for access control
                 and query scheduling.
    :param source: associated with the query. It is useful for access
                   control and query scheduling.
    :param catalog: to query. The *catalog* is associated with a Presto
                    connector. This variable sets the default catalog used
                    by SQL statements. For example, if *catalog* is set
                    to ``some_catalog``, the SQL statement
                    ``SELECT * FROM some_schema.some_table`` will actually
                    query the table
                    ``some_catalog.some_schema.some_table``.
    :param schema: to query. The *schema* is a logical abstraction to group
                   table. This variable sets the default schema used by
                   SQL statements. For eample, if *schema* is set to
                   ``some_schema``, the SQL statement
                   ``SELECT * FROM some_table`` will actually query the
                   table ``some_catalog.some_schema.some_table``.
    :param session_properties: set specific Presto behavior for the current
                               session. Please refer to the output of
                               ``SHOW SESSION`` to check the available
                               properties.
    :param http_headers: HTTP headers to post/get in the HTTP requests
    :param http_scheme: "http" or "https"
    :param auth: class that manages user authentication. ``None`` means no
                 authentication.
    :max_attempts: maximum number of attempts when sending HTTP requests. An
                   attempt is an HTTP request. 5 attempts means 4 retries.

    The client initiates a query by sending an HTTP POST to the
    coordinator. It then gets a response back from the coordinator with:
    - An URI to query to get the status for the query and the remaining
      data
    - An URI to get more information about the execution of the query
    - Statistics about the current query execution

    Please refer to :class:`PrestoStatus` to access the status returned by
    :meth:`PrestoRequest.process`.

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

    def __init__(
        self,
        host,  # type: Text
        port,  # type: int
        user,  # type: Text
        source=None,  # type: Text
        catalog=None,  # type: Text
        schema=None,  # type: Text
        session_properties=None,  # type: Optional[Dict[Text, Any]]
        http_headers=None,  # type: Optional[Dict[Text, Text]]
        http_scheme=constants.HTTP,  # type: Text
        auth=constants.DEFAULT_AUTH,  # type: Optional[Any]
        max_attempts=MAX_ATTEMPTS,  # type: int
        handle_retry=exceptions.RetryWithExponentialBackoff(),
    ):
        self._client_session = ClientSession(
            catalog,
            schema,
            source,
            user,
            session_properties,
            http_headers,
        )

        self._host = host
        self._port = port
        self._next_uri = None  # type: Optional[Text]
        # mypy cannot follow module import
        self._http_session = self.http.Session()
        self._http_session.headers.update(self.http_headers)
        self._auth = auth
        if self._auth:
            if http_scheme == constants.HTTP:
                raise ValueError('cannot use authentication with HTTP')
            self._auth.set_session(self._session)

        self._handle_retry = handle_retry
        self.max_attempts = max_attempts
        self._http_scheme = http_scheme

    @property
    def http_headers(self):
        # type: () -> Dict[Text, Text]
        headers = {}

        headers[constants.HEADER_CATALOG] = self._client_session.catalog
        headers[constants.HEADER_SCHEMA] = self._client_session.schema
        headers[constants.HEADER_SOURCE] = self._client_session.source
        headers[constants.HEADER_USER] = self._client_session.user

        headers[constants.HEADER_SESSION] = ','.join(
            # ``name`` must not contain ``=``
            '{}={}'.format(name, value)
            for name, value in self._client_session.properties.items()
        )

        # merge custom http headers
        for key in self._client_session.headers:
            if key.startswith(constants.HEADER_PREFIX):
                raise ValueError('invalid HTTP header {}: must not start with "{}"'.format(
                    key,
                    constants.HEADER_PREFIX,
                ))
        headers.update(self._client_session.headers)

        return headers

    @property
    def max_attempts(self):
        # type: () -> int
        return self._max_attempts

    @max_attempts.setter
    def max_attempts(self, value):
        # type: int -> None
        self._max_attempts = value
        if value == 1:  # No retry
            self._get = self._http_session.get
            self._post = self._http_session.post
            return

        with_retry = exceptions.retry_with(
            self._handle_retry,
            exceptions=(
                exceptions.Http503Error,
                PrestoRequest.http.ConnectionError,
                PrestoRequest.http.Timeout,
            ),
            max_attempts=self._max_attempts,
        )
        self._get = with_retry(self._http_session.get)
        self._post = with_retry(self._http_session.post)

    def get_url(self, path):
        # type: Text -> Text
        destination = '{}:{}'.format(self._host, self._port)
        return urlunparse((self._http_scheme, destination, path, '', '', ''))

    @property
    def statement_url(self):
        # type: () -> Text
        return self.get_url(constants.URL_STATEMENT_PATH)

    @property
    def next_uri(self):
        # type: () -> Text
        return self._next_uri

    def post(self, sql):
        return self._post(
            self.statement_url,
            data=sql.encode('utf-8'),
            headers=self.http_headers,
        )

    def get(self, url):
        return self._get(url, headers=self.http_headers)

    def _process_error(self, error):
        error_type = error['errorType']
        if error_type == 'EXTERNAL':
            raise exceptions.PrestoExternalError(error)
        elif error_type == 'USER_ERROR':
            return exceptions.PrestoUserError(error)

        return exceptions.PrestoQueryError(error)

    def process(self, http_response):
        # type: requests.Response -> PrestoStatus:
        status_code = http_response.status_code
        # mypy cannot follow module import
        if status_code != self.http.codes.ok:  # type: ignore
            if status_code == 503:
                raise exceptions.Http503Error('service unavailable')

            raise exceptions.HttpError(
                'error {}: {}'.format(
                    http_response.status_code,
                    http_response.content,
                )
            )

        http_response.encoding = 'utf-8'
        response = http_response.json()
        logger.debug('HTTP {}: {}'.format(http_response.status_code, response))
        if 'error' in response:
            raise self._process_error(response['error'])

        if constants.HEADER_CLEAR_SESSION in http_response.headers:
            for prop in get_header_values(
                response.headers,
                constants.HEADER_CLEAR_SESSION,
            ):
                self._client_session.properties.pop(prop, None)

        if constants.HEADER_SET_SESSION in http_response.headers:
            for key, value in get_session_property_values(
                    response.headers,
                    constants.HEADER_SET_SESSION,
                ):
                self._client_session.properties[key] = value

        self._next_uri = response.get('nextUri')

        return PrestoStatus(
            id=response['id'],
            stats=response['stats'],
            info_uri=response['infoUri'],
            next_uri=self._next_uri,
            rows=response.get('data', []),
            columns=response.get('columns'),
        )


class PrestoResult(object):
    """
    Represent the result of a Presto query as an iterator on rows.

    This class implements the iterator protocol as a generator type
    https://docs.python.org/3/library/stdtypes.html#generator-types
    """

    def __init__(self, query, rows=None):
        self._query = query
        self._rows = rows or []
        self._rownumber = 0

    @property
    def rownumber(self):
        # type: () -> int
        return self._rownumber

    def __iter__(self):
        # Initial fetch from the first POST request
        for row in self._rows:
            self._rownumber += 1
            yield row
        self._rows = None

        # Subsequent fetches from GET requests until next_uri is empty.
        while not self._query.is_finished():
            rows = self._query.fetch()
            for row in rows:
                self._rownumber += 1
                logger.debug('row {}'.format(row))
                yield row


class PrestoQuery(object):
    """Represent the execution of a SQL statement by Presto."""
    def __init__(
        self,
        request,  # type: PrestoRequest
        sql,  # type: Text
    ):
        self.query_id = None
        self.state = None

        self._columns = None

        self._finished = False
        self._request = request
        self._sql = sql

    @property
    def columns(self):
        return self._columns

    def execute(self):
        # type: () -> PrestoResult
        """Initiate a Presto query by sending the SQL statement

        This is the first HTTP request sent to the coordinator.
        It sets the query_id and returns a Result object used to
        track the rows returned by the query. To fetch all rows,
        call fetch() until is_finished is true.
        """

        response = self._request.post(self._sql)
        status = self._request.process(response)
        self.query_id = status.id
        self.state = status.stats['state']
        if status.next_uri is None:
            self._finished = True
        self.result = PrestoResult(self, status.rows)
        return self.result

    def fetch(self):
        # type: () -> List[List[Any]]
        """Continue fetching data for the current query_id"""
        response = self._request.get(self._request.next_uri)
        status = self._request.process(response)
        if status.columns:
            self._columns = status.columns
        self.state = status.stats['state']
        logger.debug(status)
        if status.next_uri is None:
            self._finished = True
        return status.rows

    def is_finished(self):
        # type: () -> bool
        return self._finished
