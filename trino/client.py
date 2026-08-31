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

import atexit
import copy
import functools
import itertools
import os
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple
from typing import Union

import httpx2
from httpx2 import Client
from httpx2 import Response

import trino.logging
from trino import constants
from trino import exceptions
from trino._protocol import _DelayExponential
from trino._protocol import _InlineSegmentTO  # noqa: F401 re-export
from trino._protocol import _parse_retry_after_header  # noqa: F401 re-export
from trino._protocol import _SegmentMetadataTO  # noqa: F401 re-export
from trino._protocol import _SegmentTO  # noqa: F401 re-export
from trino._protocol import _SpooledProtocolResponseTO
from trino._protocol import _SpooledSegmentTO
from trino._protocol import _TrinoQueryBase
from trino._protocol import _TrinoRequestBase
from trino._protocol import CaseInsensitiveDict  # noqa: F401 re-export
from trino._protocol import ClientSession
from trino._protocol import CODECS_UNAVAILABLE  # noqa: F401 re-export
from trino._protocol import CompressedQueryDataDecoder  # noqa: F401 re-export
from trino._protocol import CompressedQueryDataDecoderFactory
from trino._protocol import DecodableSegment
from trino._protocol import ENCODINGS  # noqa: F401 re-export
from trino._protocol import get_header_values  # noqa: F401 re-export
from trino._protocol import get_prepared_statement_values  # noqa: F401 re-export
from trino._protocol import get_roles_values  # noqa: F401 re-export
from trino._protocol import get_session_property_values  # noqa: F401 re-export
from trino._protocol import InlineSegment
from trino._protocol import JsonQueryDataDecoder  # noqa: F401 re-export
from trino._protocol import Lz4QueryDataDecoder  # noqa: F401 re-export
from trino._protocol import needs_retry
from trino._protocol import QueryDataDecoder
from trino._protocol import retry_after_seconds
from trino._protocol import ROLE_PATTERN  # noqa: F401 re-export
from trino._protocol import Segment
from trino._protocol import SegmentType  # noqa: F401 re-export
from trino._protocol import spooling_request_headers
from trino._protocol import TrinoStatus  # noqa: F401 re-export
from trino._protocol import wire_headers
from trino._protocol import ZStdQueryDataDecoder  # noqa: F401 re-export
from trino.auth import Authentication
from trino.mapper import RowMapper

__all__ = [
    "ClientSession",
    "TrinoQuery",
    "TrinoRequest",
    "PROXIES",
    "DecodableSegment",
    "SpooledSegment",
    "InlineSegment",
    "Segment"
]

logger = trino.logging.get_logger(__name__)
executor = ThreadPoolExecutor(max_workers=4)


def close_executor():
    executor.shutdown(wait=True)


atexit.register(close_executor)

MAX_ATTEMPTS = constants.DEFAULT_MAX_ATTEMPTS
SOCKS_PROXY = os.environ.get("SOCKS_PROXY")
# httpx configures proxies at client construction time, so PROXIES uses the
# httpx mounts-style single "all://" key instead of requests' per-scheme dict.
if SOCKS_PROXY:
    PROXIES = {"all://": "socks5://" + SOCKS_PROXY}
else:
    PROXIES = {}


class _RetryWithExponentialBackoff:
    def __init__(
            self, base=0.1, exponent=2, jitter=True, max_delay=1800  # 100ms  # 30 min
    ):
        self._get_delay = _DelayExponential(base, exponent, jitter, max_delay)

    def retry(self, func, args, kwargs, err, attempt):
        delay = self._get_delay(attempt)
        sleep(delay)


class _RetryAfterSleep:
    def __init__(self, retry_after_header):
        self._retry_after_header = retry_after_header

    def retry(self):
        sleep(self._retry_after_header)


class TrinoRequest(_TrinoRequestBase):
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

    http = httpx2

    HTTP_EXCEPTIONS: Tuple[Any, ...] = (
        http.TransportError,
    )

    def __init__(
        self,
        host: str,
        port: int,
        client_session: ClientSession,
        http_session: Optional[Client] = None,
        http_scheme: Optional[str] = None,
        auth: Optional[Authentication] = constants.DEFAULT_AUTH,
        max_attempts: int = MAX_ATTEMPTS,
        request_timeout: Union[float, Tuple[float, float]] = constants.DEFAULT_REQUEST_TIMEOUT,
        handle_retry=_RetryWithExponentialBackoff(),
        verify: Union[bool, str] = True,
    ) -> None:
        super().__init__(host, port, client_session, http_scheme)

        if http_session is not None:
            self._http_session = http_session
            if auth is not None:
                self._apply_auth_to_existing_client(http_session, auth)
        else:
            self._http_session = self.create_http_client(
                verify=verify, timeout=request_timeout, auth=auth
            )
        # httpx clients expose no readable ``verify``; clients built by
        # create_http_client carry the value used at construction time.
        self._verify = getattr(self._http_session, "_trino_verify", verify)
        self._http_session.headers.update(wire_headers(self.http_headers))
        self._exceptions = self.HTTP_EXCEPTIONS
        self._auth = auth
        if self._auth:
            self._exceptions += self._auth.get_exceptions()

        self._request_timeout = self.http.Timeout(request_timeout)
        self._handle_retry = handle_retry
        self.max_attempts = max_attempts

    @classmethod
    def create_http_client(
        cls,
        verify: Union[bool, str] = True,
        timeout: Union[float, Tuple[float, float], None] = constants.DEFAULT_REQUEST_TIMEOUT,
        auth: Optional[Authentication] = None,
        **kwargs: Any,
    ) -> Client:
        """
        Build the ``httpx2.Client`` used to talk to the coordinator.

        ``verify``, ``cert`` and ``trust_env`` can only be set when an httpx
        client is constructed, so authentication implementations contribute
        constructor arguments here through ``Authentication.get_client_arguments``.
        """
        client_kwargs: Dict[str, Any] = {
            "verify": verify,
            # HTTP/2 is negotiated via ALPN on TLS connections; plain HTTP and
            # servers without h2 support silently fall back to HTTP/1.1.
            "http2": True,
            # requests followed redirects on GET/POST/DELETE by default; httpx does not.
            "follow_redirects": True,
            "timeout": cls.http.Timeout(timeout),
        }
        if PROXIES:
            client_kwargs["proxy"] = PROXIES.get("all://")
        auth_arguments: Dict[str, Any] = {}
        http_auth = None
        if auth is not None:
            auth_arguments = auth.get_client_arguments()
            client_kwargs.update(auth_arguments)
            http_auth = auth.get_http_auth()
        client_kwargs.update(kwargs)
        client = cls.http.Client(auth=http_auth, **client_kwargs)
        client._trino_verify = client_kwargs["verify"]
        client._trino_client_arguments = frozenset(auth_arguments)
        return client

    @staticmethod
    def _apply_auth_to_existing_client(http_session: Client, auth: Authentication) -> None:
        """
        Attach ``auth`` to an already-constructed client. Only ``client.auth``
        is settable after construction; when the authentication needs
        constructor-only options (verify/cert/trust_env) that the client was
        not built with, fail loudly instead of silently ignoring them.
        """
        required = auth.get_client_arguments()
        provided = getattr(http_session, "_trino_client_arguments", frozenset())
        missing = set(required) - set(provided)
        if missing:
            raise exceptions.TrinoConnectionError(
                f"{type(auth).__name__} requires HTTP client construction options {sorted(missing)}; "
                "configure them on your own httpx2 client, or omit http_session to let the "
                "client be created for you."
            )
        http_session.auth = auth.get_http_auth()

    def unauthenticated(self):
        return TrinoRequest(
            host=self._host,
            port=self._port,
            max_attempts=self.max_attempts,
            request_timeout=self._request_timeout,
            handle_retry=self._handle_retry,
            client_session=ClientSession(user=self._client_session.user),
            verify=self._verify)

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    @max_attempts.setter
    def max_attempts(self, value: int) -> None:
        self._max_attempts = value
        if value == 1:  # No retry
            self._get = self._http_session.get
            self._post = self._http_session.post
            self._delete = self._http_session.delete
            self._head = self._http_session.head
            return

        with_retry = _retry_with(
            self._handle_retry,
            handled_exceptions=self._exceptions,
            # Retry when there is no exception but the response is a transient
            # error status or an empty 200 body; see _protocol.needs_retry.
            conditions=(needs_retry,),
            max_attempts=self._max_attempts,
        )
        self._get = with_retry(self._http_session.get)
        self._post = with_retry(self._http_session.post)
        self._delete = with_retry(self._http_session.delete)
        self._head = with_retry(self._http_session.head)

    def post(self, sql: str, additional_http_headers: Optional[Dict[str, Any]] = None) -> Response:
        data = sql.encode("utf-8")
        # Deep copy of the http_headers dict since they may be modified for this
        # request by the provided additional_http_headers
        http_headers = copy.deepcopy(self.http_headers)

        # Update the request headers with the additional_http_headers
        http_headers.update(additional_http_headers or {})

        # The Trino protocol expects UTF-8 encoded SQL text. Send the charset
        # explicitly to match the Trino JDBC client. Users may still override it.
        http_headers.setdefault(constants.HEADER_CONTENT_TYPE, constants.CONTENT_TYPE_TEXT_UTF8)

        http_response = self._post(
            self.statement_url,
            content=data,
            headers=wire_headers(http_headers),
            timeout=self._request_timeout,
        )
        return http_response

    def get(self, url: str) -> Response:
        return self._get(
            url,
            headers=wire_headers(self.http_headers),
            timeout=self._request_timeout,
        )

    def delete(self, url: str) -> Response:
        return self._delete(url, timeout=self._request_timeout)

    def head(self, url: str) -> Response:
        return self._head(
            url,
            headers=wire_headers(self.http_headers),
            timeout=self._request_timeout,
            # requests never followed redirects for HEAD; keep that behavior.
            follow_redirects=False,
        )


class TrinoResult:
    """
    Represent the result of a Trino query as an iterator on rows.

    This class implements the iterator protocol on the instance itself instead
    of as a generator. A generator that raises an exception is finalized and
    every subsequent next() raises StopIteration indistinguishable from normal
    exhaustion. Keeping the iteration state on the instance lets a transient
    error (e.g. a failed spooled segment download) propagate to the caller
    while the iterator stays usable so a retried next() resumes where the
    failure happened instead of silently dropping the remaining rows.
    """

    def __init__(self, query, rows: List[Any]):
        self._query = query
        # Initial rows from the first POST request
        self._rows = rows
        self._rownumber = 0
        # Iterator over the batch of rows currently being served
        self._current_batch: Optional[Iterator[Any]] = None
        # Rows prefetched while the current batch is being served
        self._next_rows: Optional[Any] = None

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
        return self

    def __next__(self):
        while True:
            if self._current_batch is None:
                if self._query.finished and self._rows is None:
                    raise StopIteration
                # A query only transitions to a FINISHED state when the results are fully consumed:
                # The reception of the data is acknowledged by calling the next_uri before exposing the data through
                # dbapi.
                self._next_rows = self._query.fetch() if not self._query.finished else None
                self._current_batch = iter(self._rows)

            try:
                row = next(self._current_batch)
            except StopIteration:
                self._rows = self._next_rows
                self._next_rows = None
                self._current_batch = None
                continue
            self._rownumber += 1
            return row


class TrinoQuery(_TrinoQueryBase):
    """Represent the execution of a SQL statement by Trino."""

    def __init__(
            self,
            request: TrinoRequest,
            query: str,
            legacy_primitive_types: bool = False,
            fetch_mode: Literal["mapped", "segments"] = "mapped",
            stats_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> None:
        super().__init__(query, legacy_primitive_types, fetch_mode, stats_callback)
        self._request = request
        self._result: Optional[TrinoResult] = None

    @property
    def columns(self):
        if self.query_id:
            while not self._columns and not self.finished and not self.cancelled:
                # Columns are not returned immediately after query is submitted.
                # Continue fetching data until columns information is available and push fetched rows into buffer.
                #
                # Two protocols produce rows differently:
                #  - Direct: fetch() returns a list - accumulate into the existing list.
                #  - Spooling: fetch() returns a lazy iterator - replace rows and stop,
                #    because we cannot cheaply check iterator length.
                new_rows = self.fetch()
                if isinstance(new_rows, list):
                    self._result.rows += new_rows
                else:
                    try:
                        first_row = next(new_rows)
                        self._result.rows = itertools.chain([first_row], new_rows)
                        break
                    except StopIteration:
                        self._result.rows = []
        return self._columns

    @property
    def result(self):
        return self._result

    def execute(self, additional_http_headers=None) -> TrinoResult:
        """Initiate a Trino query by sending the SQL statement

        This is the first HTTP request sent to the coordinator.
        It sets the query_id and returns a Result object used to
        track the rows returned by the query. To fetch all rows,
        call fetch() until finished is true.
        """
        if self.cancelled:
            raise exceptions.TrinoUserError("Query has been cancelled", self.query_id)

        try:
            response = self._request.post(self._query, additional_http_headers)
        except httpx2.HTTPError as e:
            raise trino.exceptions.TrinoConnectionError("failed to execute: {}".format(e))
        status = self._request.process(response)
        self._info_uri = status.info_uri
        self._query_id = status.id
        self._stats.update({"queryId": self.query_id})
        self._update_state(status)
        self._warnings = getattr(status, "warnings", [])
        if status.next_uri is None:
            self._finished = True

        rows = self._row_mapper.map(status.rows) if self._row_mapper else status.rows
        self._result = TrinoResult(self, rows)

        # Block until rows are available, the query finishes, or it is canceled.
        # Rows start as an empty list. Early responses often contain only stats,
        # so we keep fetching until actual data arrives.
        #
        # Two protocols produce rows differently:
        #  - Direct: fetch() returns a list - accumulate into the existing list.
        #  - Spooling: fetch() returns a lazy iterator - replace rows and stop,
        #    because we cannot cheaply check iterator length.
        while not self.finished and not self.cancelled and self._result.rows == []:
            new_rows = self.fetch()
            if isinstance(new_rows, list):
                self._result.rows += new_rows
            else:
                try:
                    first_row = next(new_rows)
                    self._result.rows = itertools.chain([first_row], new_rows)
                    break
                except StopIteration:
                    self._result.rows = []

        # Update statements (INSERT/UPDATE/DELETE/DDL/...) report their affected
        # row count as a single synthetic row, but Trino still returns a final
        # nextUri that must be consumed for the query to reach a terminal state.
        # Unlike a SELECT there is no result set to stream, so drain the remaining
        # pages now. Otherwise closing the cursor without fetching would issue a
        # DELETE against an already-completed statement, which Trino reports as
        # USER_CANCELED (see https://github.com/trinodb/trino-python-client/issues/601).
        #
        # Under the spooling protocol either side can be a lazy iterator. Concatenate only
        # when both are lists. Otherwise chain them, since materializing would download
        # every remaining segment.
        while self._update_type is not None and not self.finished and not self.cancelled:
            new_rows = self.fetch()
            if isinstance(self._result.rows, list) and isinstance(new_rows, list):
                self._result.rows += new_rows
            else:
                self._result.rows = itertools.chain(self._result.rows, new_rows)

        return self._result

    def fetch(self) -> Union[List[Union[List[Any], Any]], Iterator[List[Any]]]:
        """Continue fetching data for the current query_id"""
        try:
            response = self._request.get(self._request.next_uri)
        except httpx2.HTTPError as e:
            raise trino.exceptions.TrinoConnectionError("failed to fetch: {}".format(e))
        status = self._request.process(response)
        self._update_state(status)
        if status.next_uri is None:
            self._finished = True

        if not self._row_mapper:
            return []

        rows = status.rows
        if isinstance(status.rows, dict):
            # spooling protocol
            rows = cast(_SpooledProtocolResponseTO, rows)
            spooled = self._to_segments(rows)
            if self._fetch_mode == "segments":
                return spooled
            # Return iterator directly, do NOT materialize with list()
            return SegmentIterator(
                spooled,
                self._row_mapper,
                request=self._request,
                heartbeat_interval=self._request._client_session.heartbeat_interval,
            )
        elif isinstance(status.rows, list):
            return self._row_mapper.map(rows)
        else:
            raise ValueError(f"Unexpected type: {type(status.rows)}")

    def _create_spooled_segment(self, segment: _SpooledSegmentTO) -> Segment:
        return SpooledSegment(
            segment,
            self._request.unauthenticated(),
            coordinator_host=self._request._host,
            custom_headers=dict(self._request._client_session.headers),
        )

    def cancel(self) -> None:
        """Cancel the current query"""
        if self._next_uri is None:
            return

        logger.debug("cancelling query: %s", self.query_id)
        try:
            response = self._request.delete(self._next_uri)
        except httpx2.HTTPError as e:
            raise trino.exceptions.TrinoConnectionError("failed to cancel query: {}".format(e))
        if response.status_code == httpx2.codes.NO_CONTENT:
            self._cancelled = True
            logger.debug("query cancelled: %s", self.query_id)
            return

        self._request.raise_response_error(response)


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
                        retry_after = retry_after_seconds(result)
                        if retry_after is not None:
                            handle_retry_sleep = _RetryAfterSleep(retry_after)
                            handle_retry_sleep.retry()
                        else:
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


class SpooledSegment(Segment):
    """
    A subclass of Segment that handles spooled data segments, where data may be compressed and needs to be
    retrieved via HTTP requests. The segment includes methods for acknowledging processing and loading the
    segment from remote storage.

    Attributes:
        rows (property): The data, loaded and mapped from the spooled segment.
        uri (property): The URI for the spooled segment.
        ack_uri (property): The URI for acknowledging the processing of the spooled segment.
        headers (property): The headers associated with the spooled segment.

    Methods:
        acknowledge(): Sends an acknowledgment request for the segment.
    """
    def __init__(
        self,
        segment: _SpooledSegmentTO,
        request: TrinoRequest,
        coordinator_host: Optional[str] = None,
        custom_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(segment)
        self._segment = cast(_SpooledSegmentTO, segment)
        self._request = request
        self._coordinator_host = coordinator_host
        self._custom_headers = custom_headers or {}

    @property
    def data(self) -> bytes:
        # No timeout on the data path: downloading a large segment from
        # external storage may legitimately take a long time.
        http_response = self._send_spooling_request(self.uri, timeout=None)
        if http_response.is_error:
            self._request.raise_response_error(http_response)
        return http_response.content

    @property
    def uri(self) -> str:
        return self._segment["uri"]

    @property
    def ack_uri(self) -> str:
        return self._segment["ackUri"]

    @property
    def headers(self) -> Dict[str, List[str]]:
        return self._segment.get("headers", {})

    def acknowledge(self) -> None:
        def acknowledge_request():
            try:
                http_response = self._send_spooling_request(self.ack_uri, timeout=2)
                if http_response.is_error:
                    self._request.raise_response_error(http_response)
            except Exception as e:
                logger.error(f"Failed to acknowledge spooling request for segment {self}: {e}")
        # Start the acknowledgment in the executor thread
        executor.submit(acknowledge_request)

    def _send_spooling_request(self, uri: str, **kwargs) -> Response:
        headers = spooling_request_headers(uri, self.headers, self._custom_headers, self._coordinator_host)
        return self._request._get(uri, headers=headers, **kwargs)

    def __repr__(self):
        return (
            f"SpooledSegment(metadata={self.metadata})"
        )


class _RequestHeartbeat:
    """
    Heartbeat loop for a trino request. Periodically sends HEAD requests to the request's next URI.
    This prevents the coordinator from abandoning a query if the client is silent for a longer
    period of time, for example when downloading a spooled segment from an external storage.
    """
    MAX_FAILURES = 3

    def __init__(self, request: TrinoRequest, interval: float) -> None:
        self._request = request
        self._interval = interval
        # The event for telling the heartbeat thread to exit
        self._stop_event = threading.Event()

    def __enter__(self) -> _RequestHeartbeat:
        threading.Thread(target=self._run, daemon=True).start()
        return self

    def __exit__(self, *_) -> None:
        self._stop_event.set()

    def _run(self) -> None:
        """
        Run the heartbeat loop.

        Exit when the self._stop_event is set, the query completed
        or if the error count exceeds _MAX_FAILURES.
        """
        failures = 0

        while not self._stop_event.wait(timeout=self._interval):
            uri = self._request.next_uri
            if uri is None:
                return

            try:
                response = self._request.head(uri)
                if response.status_code in (404, 405):
                    logger.warning("The server does not support heartbeat calls")
                    return
                if response.is_error:
                    failures += 1
                else:
                    failures = 0
            except Exception:
                failures += 1

            if failures >= self.MAX_FAILURES:
                logger.warning(f"Stopping the heartbeat after {self.MAX_FAILURES} consecutive errors")
                return


class SegmentIterator:
    def __init__(
        self,
        segments: Union[DecodableSegment, List[DecodableSegment]],
        mapper: RowMapper,
        *,
        request: Optional[TrinoRequest] = None,
        heartbeat_interval: Optional[float] = None,
    ) -> None:
        self._segments = iter(segments if isinstance(segments, List) else [segments])
        self._mapper = mapper
        self._decoder = None
        self._rows: Iterator[List[List[Any]]] = iter([])
        self._finished = False
        self._current_segment: Optional[DecodableSegment] = None
        # Segment whose decoding failed. Retried on the next call instead of being acknowledged and skipped.
        self._pending_segment: Optional[DecodableSegment] = None
        if (request is not None) != bool(heartbeat_interval):
            raise ValueError("request and heartbeat_interval must be both provided or both omitted")
        self._request = request
        self._heartbeat_interval = heartbeat_interval

    def __iter__(self) -> Iterator[List[Any]]:
        return self

    def __next__(self) -> List[Any]:
        # If rows are exhausted, fetch the next segment
        while True:
            try:
                return next(self._rows)
            except StopIteration:
                if self._finished:
                    raise StopIteration
                self._load_next_segment()

    def _load_next_segment(self):
        # A segment is acknowledged only after its rows were decoded successfully. If the previous attempt failed
        # mid-decode (e.g. the spooled segment download failed) the same segment is retried instead of being skipped.
        if self._pending_segment is None:
            if self._current_segment:
                segment = self._current_segment.segment
                if isinstance(segment, SpooledSegment):
                    segment.acknowledge()
                self._current_segment = None

            try:
                self._pending_segment = next(self._segments)
            except StopIteration:
                self._finished = True
                return

        if self._decoder is None:
            self._decoder = SegmentDecoder(CompressedQueryDataDecoderFactory(self._mapper)
                                           .create(self._pending_segment.encoding))

        if isinstance(self._pending_segment.segment, SpooledSegment) and self._request and self._heartbeat_interval:
            # Downloading a spooled segment may take some time. In the meantime, send heartbeat
            # requests so the coordinator doesn't think we lost interest and close the query.
            with _RequestHeartbeat(self._request, self._heartbeat_interval):
                rows = self._decoder.decode(self._pending_segment.segment)
        else:
            rows = self._decoder.decode(self._pending_segment.segment)

        self._rows = iter(rows)
        self._current_segment = self._pending_segment
        self._pending_segment = None


class SegmentDecoder():
    def __init__(self, decoder: QueryDataDecoder):
        self._decoder = decoder

    def decode(self, segment: Segment) -> List[List[Any]]:
        if isinstance(segment, InlineSegment):
            inline_segment = cast(InlineSegment, segment)
            return self._decoder.decode(inline_segment.data, inline_segment.metadata)
        elif isinstance(segment, SpooledSegment):
            spooled_data = cast(SpooledSegment, segment)
            return self._decoder.decode(spooled_data.data, spooled_data.metadata)
        else:
            raise ValueError(f"Unsupported segment type: {type(segment)}")
