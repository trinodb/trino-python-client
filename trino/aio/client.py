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
Asynchronous variant of :mod:`trino.client` built on ``httpx2.AsyncClient``.

The protocol logic (headers, response processing, retry decisions, segment
decoding) is shared with the synchronous client through
:mod:`trino._protocol`; this module only contains the ``await``-shaped
control flow.
"""
from __future__ import annotations

import asyncio
import copy
import functools
from collections.abc import AsyncIterator
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

import httpx2
from httpx2 import AsyncClient
from httpx2 import Response

import trino.logging
from trino import constants
from trino import exceptions
from trino._protocol import _DelayExponential
from trino._protocol import _SpooledProtocolResponseTO
from trino._protocol import _SpooledSegmentTO
from trino._protocol import _TrinoQueryBase
from trino._protocol import _TrinoRequestBase
from trino._protocol import ClientSession
from trino._protocol import CompressedQueryDataDecoderFactory
from trino._protocol import DecodableSegment
from trino._protocol import InlineSegment
from trino._protocol import needs_retry
from trino._protocol import retry_after_seconds
from trino._protocol import Segment
from trino._protocol import spooling_request_headers
from trino._protocol import wire_headers
from trino.auth import Authentication
from trino.client import MAX_ATTEMPTS
from trino.client import PROXIES
from trino.mapper import RowMapper

__all__ = [
    "AsyncTrinoQuery",
    "AsyncTrinoRequest",
    "AsyncTrinoResult",
    "AsyncSegmentIterator",
    "AsyncSpooledSegment",
]

logger = trino.logging.get_logger(__name__)


class _AsyncRetryWithExponentialBackoff:
    def __init__(
            self, base=0.1, exponent=2, jitter=True, max_delay=1800  # 100ms  # 30 min
    ):
        self._get_delay = _DelayExponential(base, exponent, jitter, max_delay)

    async def retry(self, func, args, kwargs, err, attempt):
        await asyncio.sleep(self._get_delay(attempt))


def _retry_with_async(handle_retry, handled_exceptions, conditions, max_attempts):
    def wrapper(func):
        @functools.wraps(func)
        async def decorated(*args, **kwargs):
            error = None
            result = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = await func(*args, **kwargs)
                    if any(guard(result) for guard in conditions):
                        retry_after = retry_after_seconds(result)
                        if retry_after is not None:
                            await asyncio.sleep(retry_after)
                        else:
                            await handle_retry.retry(func, args, kwargs, None, attempt)
                        continue
                    return result
                except Exception as err:
                    error = err
                    if any(isinstance(err, exc) for exc in handled_exceptions):
                        await handle_retry.retry(func, args, kwargs, err, attempt)
                        continue
                    break
            logger.info("failed after %s attempts", attempt)
            if error is not None:
                raise error
            return result

        return decorated

    return wrapper


class AsyncTrinoRequest(_TrinoRequestBase):
    """
    Asynchronous twin of :class:`trino.client.TrinoRequest` on an
    ``httpx2.AsyncClient``. The constructor parameters are identical; the HTTP
    verbs are coroutines.
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
        http_session: Optional[AsyncClient] = None,
        http_scheme: Optional[str] = None,
        auth: Optional[Authentication] = constants.DEFAULT_AUTH,
        max_attempts: int = MAX_ATTEMPTS,
        request_timeout: Union[float, Tuple[float, float]] = constants.DEFAULT_REQUEST_TIMEOUT,
        handle_retry=_AsyncRetryWithExponentialBackoff(),
        verify: Union[bool, str] = True,
        task_registry: Optional[Set[asyncio.Task]] = None,
    ) -> None:
        super().__init__(host, port, client_session, http_scheme)

        self._owns_http_session = http_session is None
        if http_session is not None:
            self._http_session = http_session
            if auth is not None:
                self._apply_auth_to_existing_client(http_session, auth)
        else:
            self._http_session = self.create_http_client(
                verify=verify, timeout=request_timeout, auth=auth
            )
        self._verify = getattr(self._http_session, "_trino_verify", verify)
        self._http_session.headers.update(wire_headers(self.http_headers))
        self._exceptions = self.HTTP_EXCEPTIONS
        self._auth = auth
        if self._auth:
            self._exceptions += self._auth.get_exceptions()

        self._request_timeout = self.http.Timeout(request_timeout)
        self._handle_retry = handle_retry
        # Fire-and-forget acknowledgment tasks; kept strongly referenced here
        # and awaited by drain()/aclose() so none is garbage collected mid-run.
        self._task_registry: Set[asyncio.Task] = task_registry if task_registry is not None else set()
        # Requests spawned by unauthenticated(); their clients are closed with this one.
        self._children: List[AsyncTrinoRequest] = []
        self.max_attempts = max_attempts

    @classmethod
    def create_http_client(
        cls,
        verify: Union[bool, str] = True,
        timeout: Union[float, Tuple[float, float], None] = constants.DEFAULT_REQUEST_TIMEOUT,
        auth: Optional[Authentication] = None,
        **kwargs: Any,
    ) -> AsyncClient:
        """Async twin of :meth:`trino.client.TrinoRequest.create_http_client`."""
        client_kwargs: Dict[str, Any] = {
            "verify": verify,
            "http2": True,
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
        client = cls.http.AsyncClient(auth=http_auth, **client_kwargs)
        client._trino_verify = client_kwargs["verify"]
        client._trino_client_arguments = frozenset(auth_arguments)
        return client

    @staticmethod
    def _apply_auth_to_existing_client(http_session: AsyncClient, auth: Authentication) -> None:
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

    def unauthenticated(self) -> AsyncTrinoRequest:
        request = AsyncTrinoRequest(
            host=self._host,
            port=self._port,
            max_attempts=self.max_attempts,
            request_timeout=self._request_timeout,
            handle_retry=self._handle_retry,
            client_session=ClientSession(user=self._client_session.user),
            verify=self._verify,
            task_registry=self._task_registry)
        self._children.append(request)
        return request

    def register_task(self, coro) -> asyncio.Task:
        """Run a fire-and-forget coroutine, keeping a strong reference to the
        task until it completes so it survives garbage collection."""
        task = asyncio.ensure_future(coro)
        self._task_registry.add(task)
        task.add_done_callback(self._task_registry.discard)
        return task

    async def drain(self) -> None:
        """Wait for pending background tasks and close spawned child clients."""
        pending = list(self._task_registry)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        children, self._children = self._children, []
        for child in children:
            await child.aclose()

    async def aclose(self) -> None:
        await self.drain()
        if self._owns_http_session:
            await self._http_session.aclose()

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

        with_retry = _retry_with_async(
            self._handle_retry,
            handled_exceptions=self._exceptions,
            conditions=(needs_retry,),
            max_attempts=self._max_attempts,
        )
        self._get = with_retry(self._http_session.get)
        self._post = with_retry(self._http_session.post)
        self._delete = with_retry(self._http_session.delete)
        self._head = with_retry(self._http_session.head)

    async def post(self, sql: str, additional_http_headers: Optional[Dict[str, Any]] = None) -> Response:
        data = sql.encode("utf-8")
        http_headers = copy.deepcopy(self.http_headers)
        http_headers.update(additional_http_headers or {})
        http_headers.setdefault(constants.HEADER_CONTENT_TYPE, constants.CONTENT_TYPE_TEXT_UTF8)

        return await self._post(
            self.statement_url,
            content=data,
            headers=wire_headers(http_headers),
            timeout=self._request_timeout,
        )

    async def get(self, url: str) -> Response:
        return await self._get(
            url,
            headers=wire_headers(self.http_headers),
            timeout=self._request_timeout,
        )

    async def delete(self, url: str) -> Response:
        return await self._delete(url, timeout=self._request_timeout)

    async def head(self, url: str) -> Response:
        return await self._head(
            url,
            headers=wire_headers(self.http_headers),
            timeout=self._request_timeout,
            follow_redirects=False,
        )


async def _aiterate(rows) -> AsyncIterator[List[Any]]:
    """Iterate rows that may be a list or an asynchronous iterable."""
    if hasattr(rows, "__aiter__"):
        async for row in rows:
            yield row
    else:
        for row in rows:
            yield row


async def _achain(*iterables) -> AsyncIterator[List[Any]]:
    """Chain multiple (possibly asynchronous) iterables of rows."""
    for rows in iterables:
        async for row in _aiterate(rows):
            yield row


async def _aprepend(first_row, rest) -> AsyncIterator[List[Any]]:
    yield first_row
    async for row in _aiterate(rest):
        yield row


class AsyncTrinoResult:
    """
    Asynchronous twin of :class:`trino.client.TrinoResult`: an async iterator
    over the rows of a query. Iteration state lives on the instance so a
    transient error (e.g. a failed spooled segment download) propagates to the
    caller while the iterator stays usable.
    """

    def __init__(self, query: AsyncTrinoQuery, rows) -> None:
        self._query = query
        self._rows = rows
        self._rownumber = 0
        self._current_batch: Optional[AsyncIterator[Any]] = None
        self._next_rows = None

    @property
    def rows(self):
        return self._rows

    @rows.setter
    def rows(self, rows):
        self._rows = rows

    @property
    def rownumber(self) -> int:
        return self._rownumber

    def __aiter__(self) -> AsyncTrinoResult:
        return self

    async def __anext__(self):
        while True:
            if self._current_batch is None:
                if self._query.finished and self._rows is None:
                    raise StopAsyncIteration
                self._next_rows = await self._query.fetch() if not self._query.finished else None
                self._current_batch = _aiterate(self._rows)

            try:
                row = await self._current_batch.__anext__()
            except StopAsyncIteration:
                self._rows = self._next_rows
                self._next_rows = None
                self._current_batch = None
                continue
            self._rownumber += 1
            return row


class AsyncTrinoQuery(_TrinoQueryBase):
    """Represent the asynchronous execution of a SQL statement by Trino."""

    def __init__(
            self,
            request: AsyncTrinoRequest,
            query: str,
            legacy_primitive_types: bool = False,
            fetch_mode: Literal["mapped", "segments"] = "mapped",
            stats_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> None:
        super().__init__(query, legacy_primitive_types, fetch_mode, stats_callback)
        self._request = request
        self._result: Optional[AsyncTrinoResult] = None

    @property
    def columns(self):
        """The columns of the result set, as known after execute() finished.

        Unlike the synchronous client this does not lazily fetch further pages;
        execute() already polls until rows or a terminal state arrived.
        """
        return self._columns

    @property
    def result(self):
        return self._result

    async def execute(self, additional_http_headers=None) -> AsyncTrinoResult:
        """Initiate a Trino query by sending the SQL statement

        This is the first HTTP request sent to the coordinator.
        It sets the query_id and returns a Result object used to
        track the rows returned by the query.
        """
        if self.cancelled:
            raise exceptions.TrinoUserError("Query has been cancelled", self.query_id)

        try:
            response = await self._request.post(self._query, additional_http_headers)
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
        self._result = AsyncTrinoResult(self, rows)

        # Block until rows are available, the query finishes, or it is
        # canceled; see TrinoQuery.execute for the direct-vs-spooling shape
        # of these loops.
        while not self.finished and not self.cancelled and self._result.rows == []:
            new_rows = await self.fetch()
            if isinstance(new_rows, list):
                self._result.rows += new_rows
            else:
                try:
                    first_row = await new_rows.__anext__()
                    self._result.rows = _aprepend(first_row, new_rows)
                    break
                except StopAsyncIteration:
                    self._result.rows = []

        # Drain the trailing pages of update statements so the query reaches a
        # terminal state; see TrinoQuery.execute for the full rationale.
        while self._update_type is not None and not self.finished and not self.cancelled:
            new_rows = await self.fetch()
            if isinstance(self._result.rows, list) and isinstance(new_rows, list):
                self._result.rows += new_rows
            else:
                self._result.rows = _achain(self._result.rows, new_rows)

        return self._result

    async def fetch(self) -> Union[List[Union[List[Any], Any]], AsyncIterator[List[Any]]]:
        """Continue fetching data for the current query_id"""
        try:
            response = await self._request.get(self._request.next_uri)
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
            return AsyncSegmentIterator(
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
        return AsyncSpooledSegment(
            segment,
            self._request.unauthenticated(),
            coordinator_host=self._request._host,
            custom_headers=dict(self._request._client_session.headers),
        )

    async def cancel(self) -> None:
        """Cancel the current query"""
        if self._next_uri is None:
            return

        logger.debug("cancelling query: %s", self.query_id)
        try:
            response = await self._request.delete(self._next_uri)
        except httpx2.HTTPError as e:
            raise trino.exceptions.TrinoConnectionError("failed to cancel query: {}".format(e))
        if response.status_code == httpx2.codes.NO_CONTENT:
            self._cancelled = True
            logger.debug("query cancelled: %s", self.query_id)
            return

        self._request.raise_response_error(response)


class AsyncSpooledSegment(Segment):
    """
    Asynchronous twin of :class:`trino.client.SpooledSegment`: the segment data
    is downloaded with ``await segment.data()`` and acknowledgments run as
    asyncio tasks registered on the owning request.
    """

    def __init__(
        self,
        segment: _SpooledSegmentTO,
        request: AsyncTrinoRequest,
        coordinator_host: Optional[str] = None,
        custom_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(segment)
        self._segment = cast(_SpooledSegmentTO, segment)
        self._request = request
        self._coordinator_host = coordinator_host
        self._custom_headers = custom_headers or {}

    async def data(self) -> bytes:
        # No timeout on the data path: downloading a large segment from
        # external storage may legitimately take a long time.
        http_response = await self._send_spooling_request(self.uri, timeout=None)
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
        async def acknowledge_request():
            try:
                http_response = await self._send_spooling_request(self.ack_uri, timeout=2)
                if http_response.is_error:
                    self._request.raise_response_error(http_response)
            except Exception as e:
                logger.error(f"Failed to acknowledge spooling request for segment {self}: {e}")

        # Fire and forget; the task is awaited when the connection is closed.
        self._request.register_task(acknowledge_request())

    async def _send_spooling_request(self, uri: str, **kwargs) -> Response:
        headers = spooling_request_headers(uri, self.headers, self._custom_headers, self._coordinator_host)
        return await self._request._get(uri, headers=headers, **kwargs)

    def __repr__(self):
        return (
            f"AsyncSpooledSegment(metadata={self.metadata})"
        )


class _AsyncRequestHeartbeat:
    """
    Asynchronous twin of :class:`trino.client._RequestHeartbeat`: an asyncio
    task periodically sending HEAD requests to the request's next URI while a
    spooled segment downloads.
    """
    MAX_FAILURES = 3

    def __init__(self, request: AsyncTrinoRequest, interval: float) -> None:
        self._request = request
        self._interval = interval
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    async def __aenter__(self) -> _AsyncRequestHeartbeat:
        self._task = asyncio.ensure_future(self._run())
        return self

    async def __aexit__(self, *_) -> None:
        self._stop_event.set()
        if self._task is not None:
            await self._task

    async def _run(self) -> None:
        """
        Run the heartbeat loop.

        Exit when the stop event is set, the query completed or if the error
        count exceeds MAX_FAILURES.
        """
        failures = 0

        while True:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval)
                return
            except asyncio.TimeoutError:
                pass

            uri = self._request.next_uri
            if uri is None:
                return

            try:
                response = await self._request.head(uri)
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


class AsyncSegmentIterator:
    def __init__(
        self,
        segments: Union[DecodableSegment, List[DecodableSegment]],
        mapper: RowMapper,
        *,
        request: Optional[AsyncTrinoRequest] = None,
        heartbeat_interval: Optional[float] = None,
    ) -> None:
        self._segments = iter(segments if isinstance(segments, List) else [segments])
        self._mapper = mapper
        self._decoder = None
        self._rows = iter([])
        self._finished = False
        self._current_segment: Optional[DecodableSegment] = None
        # Segment whose decoding failed. Retried on the next call instead of being acknowledged and skipped.
        self._pending_segment: Optional[DecodableSegment] = None
        if (request is not None) != bool(heartbeat_interval):
            raise ValueError("request and heartbeat_interval must be both provided or both omitted")
        self._request = request
        self._heartbeat_interval = heartbeat_interval

    def __aiter__(self) -> AsyncSegmentIterator:
        return self

    async def __anext__(self) -> List[Any]:
        # If rows are exhausted, fetch the next segment
        while True:
            try:
                return next(self._rows)
            except StopIteration:
                if self._finished:
                    raise StopAsyncIteration
                await self._load_next_segment()

    async def _load_next_segment(self):
        # A segment is acknowledged only after its rows were decoded successfully. If the previous attempt failed
        # mid-decode (e.g. the spooled segment download failed) the same segment is retried instead of being skipped.
        if self._pending_segment is None:
            if self._current_segment:
                segment = self._current_segment.segment
                if isinstance(segment, AsyncSpooledSegment):
                    segment.acknowledge()
                self._current_segment = None

            try:
                self._pending_segment = next(self._segments)
            except StopIteration:
                self._finished = True
                return

        if self._decoder is None:
            self._decoder = CompressedQueryDataDecoderFactory(self._mapper) \
                .create(self._pending_segment.encoding)

        segment = self._pending_segment.segment
        if isinstance(segment, AsyncSpooledSegment):
            if self._request and self._heartbeat_interval:
                # Downloading a spooled segment may take some time. In the meantime, send heartbeat
                # requests so the coordinator doesn't think we lost interest and close the query.
                async with _AsyncRequestHeartbeat(self._request, self._heartbeat_interval):
                    data = await segment.data()
            else:
                data = await segment.data()
        elif isinstance(segment, InlineSegment):
            data = segment.data
        else:
            raise ValueError(f"Unsupported segment type: {type(segment)}")

        rows = self._decoder.decode(data, segment.metadata)
        self._rows = iter(rows)
        self._current_segment = self._pending_segment
        self._pending_segment = None
