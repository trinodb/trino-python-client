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
I/O-free pieces of the Trino HTTP protocol shared by the synchronous client
(:mod:`trino.client`) and the asynchronous client (:mod:`trino.aio`).

Everything in this module is transport-agnostic: header construction and
parsing, response processing, retry decisions, session state, and segment
decoding. The actual HTTP calls live in the transport-specific modules.
"""
from __future__ import annotations

import abc
import base64
import copy
import random
import re
import threading
import urllib.parse
import warnings
from abc import abstractmethod
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import MutableMapping
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple
from typing import TypedDict
from typing import Union
from zoneinfo import ZoneInfo

try:
    import lz4.block
except ImportError as err:
    _LZ4_ERROR: Optional[str] = str(err)
else:
    _LZ4_ERROR = None

try:
    import orjson as json
except ImportError:
    import json

try:
    import zstandard
except ImportError as err:
    _ZSTD_ERROR: Optional[str] = str(err)
else:
    _ZSTD_ERROR = None

import trino.logging
from trino import constants
from trino import exceptions
from trino._version import __version__
from trino.exceptions import TrinoExternalError
from trino.exceptions import TrinoQueryError
from trino.exceptions import TrinoUserError
from trino.mapper import RowMapper
from trino.mapper import RowMapperFactory

logger = trino.logging.get_logger(__name__)

_HEADER_EXTRA_CREDENTIAL_KEY_REGEX = re.compile(r'^\S[^\s=]*$')

ENCODINGS = ["json+zstd", "json+lz4", "json"]
CODECS_UNAVAILABLE = {}
if _LZ4_ERROR:
    CODECS_UNAVAILABLE["lz4"] = _LZ4_ERROR
if _ZSTD_ERROR:
    CODECS_UNAVAILABLE["zstd"] = _ZSTD_ERROR

ROLE_PATTERN = re.compile(r"^ROLE\{(.*)\}$")

# HTTP status codes that are retried because they are transient coordinator or
# gateway conditions: Too Many Requests, Bad Gateway, Service Unavailable and
# Gateway Timeout.
RETRIABLE_STATUS_CODES = (429, 502, 503, 504)


class CaseInsensitiveDict(MutableMapping[str, Any]):
    """
    A case-insensitive ``dict``-like object for HTTP headers.

    Keys are matched case-insensitively but the case of the last key set is
    preserved for iteration. Unlike ``httpx2.Headers`` it tolerates ``None``
    values, which the client uses to mean "header not sent"; ``None``-valued
    entries are filtered out before a request hits the wire.
    """

    def __init__(self, data: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
        # Maps lowercased key -> (actual key, value)
        self._store: Dict[str, Tuple[str, Any]] = {}
        if data is not None:
            self.update(data)
        self.update(kwargs)

    def __setitem__(self, key: str, value: Any) -> None:
        self._store[key.lower()] = (key, value)

    def __getitem__(self, key: str) -> Any:
        return self._store[key.lower()][1]

    def __delitem__(self, key: str) -> None:
        del self._store[key.lower()]

    def __iter__(self) -> Iterator[str]:
        return (original_key for original_key, _ in self._store.values())

    def __len__(self) -> int:
        return len(self._store)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Mapping):
            other_dict = {k.lower(): v for k, v in other.items()}
            return {k: v for k, (_, v) in self._store.items()} == other_dict
        return NotImplemented

    def __repr__(self) -> str:
        return repr(dict(self.items()))

    def copy(self) -> CaseInsensitiveDict:
        return CaseInsensitiveDict(dict(self.items()))


def wire_headers(headers: Mapping[str, Any]) -> Dict[str, str]:
    """Drop ``None``-valued headers; only the remaining ones are sent on the wire."""
    return {key: value for key, value in headers.items() if value is not None}


def get_header_values(headers: Mapping[str, str], header: str) -> List[str]:
    return [val.strip() for val in headers[header].split(",")]


def get_session_property_values(headers: Mapping[str, str], header: str) -> List[Tuple[str, str]]:
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), urllib.parse.unquote_plus(v.strip()))
        for k, v in (kv.split("=", 1) for kv in kvs if kv)
    ]


def get_prepared_statement_values(headers: Mapping[str, str], header: str) -> List[Tuple[str, str]]:
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), urllib.parse.unquote_plus(v.strip()))
        for k, v in (kv.split("=", 1) for kv in kvs if kv)
    ]


def get_roles_values(headers: Mapping[str, str], header: str) -> List[Tuple[str, str]]:
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), urllib.parse.unquote_plus(v.strip()))
        for k, v in (kv.split("=", 1) for kv in kvs if kv)
    ]


class ClientSession:
    """
    Manage the current Client Session properties of a specific connection. This class is thread-safe.

    :param user: associated with the query. It is useful for access control
                 and query scheduling.
    :param authorization_user: associated with the query. It is useful for access control
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
    :param timezone: The timezone for query processing. Defaults to the system's local timezone.
    :param encoding: The encoding for the spooling protocol. Defaults to None.
    """

    def __init__(
        self,
        user: str,
        authorization_user: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        source: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        transaction_id: Optional[str] = None,
        extra_credential: Optional[List[Tuple[str, str]]] = None,
        client_tags: Optional[List[str]] = None,
        roles: Optional[Union[Dict[str, str], str]] = None,
        timezone: Optional[str] = None,
        encoding: Optional[Union[str, List[str]]] = None,
        heartbeat_interval: Optional[float] = constants.DEFAULT_HEARTBEAT_INTERVAL,
    ):
        self._object_lock = threading.Lock()
        self._prepared_statements: Dict[str, str] = {}

        self._user = user
        self._authorization_user = authorization_user
        self._catalog = catalog
        self._schema = schema
        self._source = source
        self._properties = properties.copy() if properties is not None else {}
        self._headers = headers.copy() if headers is not None else {}
        self._transaction_id = transaction_id
        self._extra_credential = extra_credential
        self._client_tags = client_tags.copy() if client_tags is not None else list()
        self._roles = self._format_roles(roles) if roles is not None else {}
        if timezone:  # Check timezone validity
            ZoneInfo(timezone)
            self._timezone = timezone
        else:
            from tzlocal import get_localzone_name
            self._timezone = get_localzone_name()
        self._encoding = encoding
        self._heartbeat_interval = heartbeat_interval

    @property
    def user(self) -> str:
        return self._user

    @property
    def authorization_user(self) -> Optional[str]:
        with self._object_lock:
            return self._authorization_user

    @authorization_user.setter
    def authorization_user(self, authorization_user: Optional[str]) -> None:
        with self._object_lock:
            self._authorization_user = authorization_user

    @property
    def catalog(self) -> Optional[str]:
        with self._object_lock:
            return self._catalog

    @catalog.setter
    def catalog(self, catalog: Optional[str]) -> None:
        with self._object_lock:
            self._catalog = catalog

    @property
    def schema(self) -> Optional[str]:
        with self._object_lock:
            return self._schema

    @schema.setter
    def schema(self, schema: Optional[str]) -> None:
        with self._object_lock:
            self._schema = schema

    @property
    def source(self) -> Optional[str]:
        return self._source

    @property
    def properties(self) -> Dict[str, str]:
        with self._object_lock:
            return self._properties

    @properties.setter
    def properties(self, properties: Dict[str, str]) -> None:
        with self._object_lock:
            self._properties = properties

    @property
    def headers(self) -> Dict[str, str]:
        return self._headers

    @property
    def transaction_id(self) -> Optional[str]:
        with self._object_lock:
            return self._transaction_id

    @transaction_id.setter
    def transaction_id(self, transaction_id: Optional[str]) -> None:
        with self._object_lock:
            self._transaction_id = transaction_id

    @property
    def extra_credential(self) -> Optional[List[Tuple[str, str]]]:
        return self._extra_credential

    @property
    def client_tags(self) -> List[str]:
        return self._client_tags

    @property
    def roles(self) -> Dict[str, str]:
        with self._object_lock:
            return self._roles

    @roles.setter
    def roles(self, roles: Dict[str, str]) -> None:
        with self._object_lock:
            self._roles = roles

    @property
    def prepared_statements(self) -> Dict[str, str]:
        return self._prepared_statements

    @prepared_statements.setter
    def prepared_statements(self, prepared_statements: Dict[str, str]) -> None:
        with self._object_lock:
            self._prepared_statements = prepared_statements

    @property
    def timezone(self) -> str:
        with self._object_lock:
            return self._timezone

    @property
    def encoding(self) -> Optional[Union[str, List[str]]]:
        with self._object_lock:
            return self._encoding

    @property
    def heartbeat_interval(self) -> Optional[float]:
        return self._heartbeat_interval

    @staticmethod
    def _format_roles(roles: Union[Dict[str, str], str]) -> Dict[str, str]:
        if isinstance(roles, str):
            roles = {"system": roles}
        formatted_roles = {}
        for catalog, role in roles.items():
            is_legacy_role_pattern = ROLE_PATTERN.match(role) is not None
            if role in ("NONE", "ALL") or is_legacy_role_pattern:
                if is_legacy_role_pattern:
                    warnings.warn(f"A role '{role}' is provided using a legacy format. "
                                  "Please remove the ROLE{} wrapping. Support for the legacy format might be "
                                  "removed in a future release.",
                                  DeprecationWarning)
                formatted_roles[catalog] = role
            else:
                formatted_roles[catalog] = f"ROLE{{{role}}}"
        return formatted_roles

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        del state["_object_lock"]
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._object_lock = threading.Lock()


@dataclass
class TrinoStatus:
    id: str
    stats: Dict[str, str]
    warnings: List[Any]
    info_uri: str
    next_uri: Optional[str]
    update_type: Optional[str]
    update_count: Optional[int]
    rows: Union[List[Any], Dict[str, Any]]
    columns: List[Any]

    def __repr__(self) -> str:
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


class _DelayExponential:
    def __init__(
            self,
            base: float = 0.1,  # 100ms
            exponent: float = 2,
            jitter: bool = True,
            max_delay: float = 1800,  # 30 min
    ) -> None:
        self._base = base
        self._exponent = exponent
        self._jitter = jitter
        self._max_delay = max_delay

    def __call__(self, attempt: int) -> float:
        delay = float(self._base) * (self._exponent ** attempt)
        if self._jitter:
            delay *= random.random()
        delay = min(float(self._max_delay), delay)
        return delay


def _parse_retry_after_header(retry_after: Union[int, str]) -> float:
    if isinstance(retry_after, int):
        return retry_after
    elif isinstance(retry_after, str) and retry_after.isdigit():
        return int(retry_after)
    else:
        retry_date = parsedate_to_datetime(retry_after)
        now = datetime.utcnow()
        return (retry_date - now).total_seconds()


def needs_retry(response: Any) -> bool:
    """Retry decision for a response that did not raise: transient gateway
    statuses, or a 200 with an empty body (transient under load)."""
    status_code = getattr(response, "status_code", None)
    if status_code in RETRIABLE_STATUS_CODES:
        return True
    return status_code == 200 and not getattr(response, "text", "").strip()


def retry_after_seconds(response: Any) -> Optional[float]:
    """Server-mandated delay before retrying, from a 429's Retry-After header."""
    if getattr(response, "status_code", None) == 429 and "Retry-After" in response.headers:
        return _parse_retry_after_header(response.headers.get("Retry-After"))
    return None


class _TrinoRequestBase(abc.ABC):
    """
    Transport-agnostic half of a Trino request: URL construction, protocol
    header construction, and response processing. Subclasses provide the
    actual HTTP verbs on top of an ``httpx2.Client`` or ``httpx2.AsyncClient``.
    """

    def __init__(
        self,
        host: str,
        port: int,
        client_session: ClientSession,
        http_scheme: Optional[str] = None,
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

    @property
    def transaction_id(self) -> Optional[str]:
        return self._client_session.transaction_id

    @transaction_id.setter
    def transaction_id(self, value: Optional[str]) -> None:
        self._client_session.transaction_id = value

    @property
    def http_headers(self) -> CaseInsensitiveDict:
        headers = CaseInsensitiveDict()

        headers[constants.HEADER_CATALOG] = self._client_session.catalog
        headers[constants.HEADER_SCHEMA] = self._client_session.schema
        headers[constants.HEADER_SOURCE] = self._client_session.source
        if self._client_session.authorization_user is not None:
            headers[constants.HEADER_ORIGINAL_USER] = self._client_session.user
            headers[constants.HEADER_USER] = self._client_session.authorization_user
        else:
            headers[constants.HEADER_USER] = self._client_session.user
        headers[constants.HEADER_TIMEZONE] = self._client_session.timezone
        if self._client_session.encoding is None:
            if not CODECS_UNAVAILABLE:
                pass
            else:
                encoding = [
                    enc
                    for enc in ENCODINGS
                    if (enc.split("+")[1] if "+" in enc else None) not in CODECS_UNAVAILABLE
                ]
                headers[constants.HEADER_ENCODING] = ",".join(encoding)
        elif isinstance(self._client_session.encoding, list):
            headers[constants.HEADER_ENCODING] = ",".join(self._client_session.encoding)
        elif isinstance(self._client_session.encoding, str):
            headers[constants.HEADER_ENCODING] = self._client_session.encoding
        else:
            raise ValueError("Invalid type for encoding: expected str or list")
        headers[constants.HEADER_CLIENT_CAPABILITIES] = constants.CLIENT_CAPABILITIES

        headers["user-agent"] = f"{constants.CLIENT_NAME}/{__version__}"
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
                    [f"{tup[0]}={urllib.parse.quote_plus(str(tup[1]))}"
                     for tup in self._client_session.extra_credential])

        return headers

    def get_url(self, path: str) -> str:
        return "{protocol}://{host}:{port}{path}".format(
            protocol=self._http_scheme, host=self._host, port=self._port, path=path
        )

    @property
    def statement_url(self) -> str:
        return self.get_url(constants.URL_STATEMENT_PATH)

    @property
    def next_uri(self) -> Optional[str]:
        return self._next_uri

    @staticmethod
    def _process_error(
        error: Dict[str, Any], query_id: Optional[str]
    ) -> Union[TrinoExternalError, TrinoQueryError, TrinoUserError]:
        error_type = error["errorType"]
        if error_type == "EXTERNAL":
            raise exceptions.TrinoExternalError(error, query_id)
        elif error_type == "USER_ERROR":
            return exceptions.TrinoUserError(error, query_id)

        return exceptions.TrinoQueryError(error, query_id)

    @staticmethod
    def raise_response_error(http_response: Any) -> None:
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

    def process(self, http_response: Any) -> TrinoStatus:
        if http_response.is_error:
            self.raise_response_error(http_response)

        try:
            http_response.encoding = "utf-8"
        except ValueError:
            # .text was already accessed (e.g. by the retry logic), so the body
            # has been decoded with the detected encoding already.
            pass
        if not http_response.text.strip():
            raise exceptions.TrinoConnectionError(
                "received empty response from server (status 200)"
            )
        response = json.loads(http_response.text)
        if "error" in response and response["error"]:
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

        if constants.HEADER_SET_AUTHORIZATION_USER in http_response.headers:
            self._client_session.authorization_user = http_response.headers[constants.HEADER_SET_AUTHORIZATION_USER]

        if constants.HEADER_RESET_AUTHORIZATION_USER in http_response.headers:
            self._client_session.authorization_user = None

        self._next_uri = response.get("nextUri")

        data = response.get("data") if response.get("data") else []

        return TrinoStatus(
            id=response["id"],
            stats=response["stats"],
            warnings=response.get("warnings", []),
            info_uri=response["infoUri"],
            next_uri=self._next_uri,
            update_type=response.get("updateType"),
            update_count=response.get("updateCount"),
            rows=data,
            columns=response.get("columns"),
        )

    @staticmethod
    def _verify_extra_credential(header: Tuple[str, str]) -> None:
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


class _TrinoQueryBase(abc.ABC):
    """Transport-agnostic state of the execution of a SQL statement by Trino."""

    def __init__(
            self,
            query: str,
            legacy_primitive_types: bool = False,
            fetch_mode: Literal["mapped", "segments"] = "mapped",
            stats_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> None:
        self._query_id: Optional[str] = None
        self._stats: Dict[Any, Any] = {}
        self._info_uri: Optional[str] = None
        self._warnings: List[Dict[Any, Any]] = []
        self._columns: Optional[List[str]] = None
        self._finished = False
        self._cancelled = False
        self._update_type: Optional[str] = None
        self._update_count: Optional[int] = None
        self._next_uri: Optional[str] = None
        self._query = query
        self._legacy_primitive_types = legacy_primitive_types
        self._row_mapper: Optional[Any] = None
        self._fetch_mode = fetch_mode
        self._stats_callback = stats_callback

    @property
    def query_id(self) -> Optional[str]:
        return self._query_id

    @property
    def query(self) -> Optional[str]:
        return self._query

    @property
    def stats(self) -> Dict[Any, Any]:
        return self._stats

    @property
    def update_type(self) -> Optional[str]:
        return self._update_type

    @property
    def update_count(self) -> Optional[int]:
        return self._update_count

    @property
    def warnings(self) -> List[Dict[Any, Any]]:
        return self._warnings

    @property
    def info_uri(self) -> Optional[str]:
        return self._info_uri

    @property
    def finished(self) -> bool:
        return self._finished

    def is_finished(self) -> bool:
        warnings.warn("is_finished is deprecated, use finished instead", DeprecationWarning)
        return self.finished

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def _update_state(self, status: TrinoStatus) -> None:
        self._stats.update(status.stats)
        self._update_type = status.update_type
        self._update_count = status.update_count
        self._next_uri = status.next_uri
        if not self._row_mapper and status.columns:
            self._row_mapper = RowMapperFactory().create(columns=status.columns,
                                                         legacy_primitive_types=self._legacy_primitive_types)
        if status.columns:
            self._columns = status.columns
        self._report_stats()

    def _report_stats(self) -> None:
        if self._stats_callback is not None:
            # Pass a deep copy so the callback cannot mutate internal query state.
            self._stats_callback(copy.deepcopy(self._stats))

    def _to_segments(self, rows: _SpooledProtocolResponseTO) -> List[DecodableSegment]:
        encoding = rows["encoding"]
        metadata = rows["metadata"] if "metadata" in rows else None
        segments: List[Segment] = []
        for segment in rows["segments"]:
            segment_type = segment["type"]
            if segment_type == SegmentType.INLINE:
                inline_segment = cast("_InlineSegmentTO", segment)
                segments.append(InlineSegment(inline_segment))
            elif segment_type == SegmentType.SPOOLED:
                segments.append(self._create_spooled_segment(cast("_SpooledSegmentTO", segment)))
            else:
                raise ValueError(f"Unsupported segment type: {segment_type}")

        return list(map(lambda segment: DecodableSegment(encoding, metadata, segment), segments))

    @abstractmethod
    def _create_spooled_segment(self, segment: _SpooledSegmentTO) -> Segment:
        """Build the transport-specific spooled segment (sync or async download)."""


# Trino Spooled protocol transfer objects
class _SpooledProtocolResponseTO(TypedDict):
    encoding: Literal["json", "json+std", "json+lz4"]
    metadata: _SegmentMetadataTO
    segments: List[_SegmentTO]


class _SegmentMetadataTO(TypedDict):
    uncompressedSize: str
    segmentSize: str


class _SegmentTO(_SegmentMetadataTO):
    type: Literal["spooled", "inline"]
    metadata: _SegmentMetadataTO


class _SpooledSegmentTO(_SegmentTO):
    uri: str
    ackUri: str
    headers: Dict[str, List[str]]


class _InlineSegmentTO(_SegmentTO):
    data: str


class SegmentType(str, Enum):
    """Enum with string values that can be compared to strings."""
    INLINE = "inline"
    SPOOLED = "spooled"


class Segment(abc.ABC):
    """
    Abstract base class representing a segment of data produced by the spooling protocol.

    Attributes:
        metadata (property): Metadata associated with the segment.
        rows (property): Returns the decoded and mapped data.
    """
    def __init__(self, segment: _SegmentTO) -> None:
        self._segment = segment

    @property
    def metadata(self) -> _SegmentMetadataTO:
        return self._segment["metadata"]


class InlineSegment(Segment):
    """
    A subclass of Segment that handles inline data segments. The data is base64 encoded and
    requires mapping to rows using the provided row_mapper.

    Attributes:
        rows (property): The data in the segment, decoded and mapped from the base64 encoded data.
    """
    def __init__(self, segment: _InlineSegmentTO) -> None:
        super().__init__(segment)
        self._segment = cast(_InlineSegmentTO, segment)

    @property
    def data(self) -> bytes:
        return base64.b64decode(cast(_InlineSegmentTO, self._segment)["data"])

    def __repr__(self) -> str:
        return f"InlineSegment(metadata={self.metadata})"


class DecodableSegment:
    """
    Represents a collection of spooled segments of data, with an encoding format.

    Attributes:
        encoding (str): The encoding format of the spooled data.
        metadata (_SegmentMetadataTO): Metadata for all segments in the query
        segment (Segment): The spooled segment data
    """
    def __init__(self, encoding: str, metadata: _SegmentMetadataTO, segment: Segment) -> None:
        self._encoding = encoding
        self._metadata = metadata
        self._segment = segment

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def segment(self) -> Segment:
        return self._segment

    @property
    def metadata(self) -> _SegmentMetadataTO:
        return self._metadata

    def __repr__(self) -> str:
        return (f"DecodableSegment(encoding={self._encoding}, metadata={self._metadata}, segment={self._segment})")


def spooling_request_headers(
    uri: str,
    segment_headers: Dict[str, List[str]],
    custom_headers: Dict[str, str],
    coordinator_host: Optional[str],
) -> Dict[str, str]:
    """
    Headers for a spooled segment data/ack request.

    Forward user-supplied custom headers (e.g. auth gateway headers) only when the
    request targets the Trino coordinator, never to external storage (e.g. S3 presigned
    URLs) where such headers can break the request. The per-segment protocol headers
    returned by the coordinator always take precedence.
    """
    headers: Dict[str, str] = {}
    if coordinator_host is not None and urllib.parse.urlsplit(uri).hostname == coordinator_host:
        headers.update(custom_headers)
    for key, values in segment_headers.items():
        if len(values) > 1:
            raise ValueError(f"Header '{key}' contains multiple values: {values}")
        headers[key] = values[0]
    return headers


class QueryDataDecoder(abc.ABC):
    @abstractmethod
    def decode(self, data: bytes, metadata: _SegmentMetadataTO) -> List[List[Any]]:
        pass


class JsonQueryDataDecoder(QueryDataDecoder):
    def __init__(self, mapper: RowMapper) -> None:
        self._mapper = mapper

    def decode(self, data: bytes, metadata: _SegmentMetadataTO) -> List[List[Any]]:
        return self._mapper.map(json.loads(data.decode("utf8")))


class CompressedQueryDataDecoder(QueryDataDecoder):
    def __init__(self, delegate: QueryDataDecoder) -> None:
        self._delegate = delegate

    @abstractmethod
    def decompress(self, data: bytes, metadata: _SegmentMetadataTO) -> bytes:
        pass

    def decode(self, data: bytes, metadata: _SegmentMetadataTO) -> List[List[Any]]:
        if "uncompressedSize" not in metadata:
            # Data not compressed - below threshold
            return self._delegate.decode(data, metadata)

        # Data is compressed
        expected_compressed_size = metadata["segmentSize"]
        if not len(data) == expected_compressed_size:
            raise RuntimeError(f"Expected to read {expected_compressed_size} bytes but got {len(data)}")
        decompressed_data = self.decompress(data, metadata)
        expected_uncompressed_size = metadata["uncompressedSize"]
        if not len(decompressed_data) == expected_uncompressed_size:
            raise RuntimeError(
                "Decompressed size does not match expected segment size, "
                f"expected {expected_uncompressed_size}, got {len(decompressed_data)}"
            )
        return self._delegate.decode(decompressed_data, metadata)


class ZStdQueryDataDecoder(CompressedQueryDataDecoder):
    def __init__(self, delegate: QueryDataDecoder) -> None:
        super().__init__(delegate)
        self._decompressor: Optional[Any] = None

    def decompress(self, data: bytes, metadata: _SegmentMetadataTO) -> bytes:
        if self._decompressor is None:
            self._decompressor = zstandard.ZstdDecompressor()
        return self._decompressor.decompress(data)


class Lz4QueryDataDecoder(CompressedQueryDataDecoder):
    def decompress(self, data: bytes, metadata: _SegmentMetadataTO) -> bytes:
        expected_uncompressed_size = metadata["uncompressedSize"]
        decoded_bytes = lz4.block.decompress(data, uncompressed_size=int(expected_uncompressed_size))
        return decoded_bytes


class CompressedQueryDataDecoderFactory():
    def __init__(self, mapper: RowMapper) -> None:
        self._mapper = mapper

    def create(self, encoding: str) -> QueryDataDecoder:
        if encoding == "json+zstd":
            if "zstd" in CODECS_UNAVAILABLE:
                raise ValueError(
                    f"zstd is not installed so json+zstd encoding is not supported: {CODECS_UNAVAILABLE['zstd']}"
                )
            return ZStdQueryDataDecoder(JsonQueryDataDecoder(self._mapper))
        elif encoding == "json+lz4":
            if "lz4" in CODECS_UNAVAILABLE:
                raise ValueError(
                    f"lz4 is not installed so json+lz4 encoding is not supported: {CODECS_UNAVAILABLE['lz4']}"
                )
            return Lz4QueryDataDecoder(JsonQueryDataDecoder(self._mapper))
        elif encoding == "json":
            return JsonQueryDataDecoder(self._mapper)
        else:
            raise ValueError(f"Unsupported encoding: {encoding}")
