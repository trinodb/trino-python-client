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
Hand-written HTTP mocking for the unit tests, replacing httpretty.

``MockTrinoServer`` is a recording router served through ``httpx2.MockTransport``.
Handlers receive the ``httpx2.Request`` and return an ``httpx2.Response``; every
request is recorded so tests can assert on what actually went on the wire. One
server instance can serve synchronous and asynchronous clients alike.
"""
from __future__ import annotations

import json as _json
import re
import threading
from re import Pattern
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import httpx2

Handler = Callable[[httpx2.Request], httpx2.Response]


class MockTrinoServer:
    def __init__(self) -> None:
        self._routes: List[tuple[str, Union[str, Pattern[str]], Handler]] = []
        self._requests: List[httpx2.Request] = []
        # The handler can be hit concurrently from many threads or tasks.
        self._lock = threading.Lock()

    def register(
        self,
        method: str,
        path: Union[str, Pattern[str]],
        handler: Optional[Handler] = None,
        *,
        json: Any = None,
        text: Optional[str] = None,
        status: int = 200,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Register a route. ``path`` is matched exactly (string) or by regex
        ``search`` (compiled pattern) against the request path. Instead of a
        ``handler`` callable, a static ``json``/``text`` body with ``status``
        and ``headers`` can be given. Like httpretty, the most recently
        registered matching route wins.
        """
        if handler is None:
            static_json = json
            static_text = text
            static_status = status
            static_headers = headers or {}

            def handler(request: httpx2.Request) -> httpx2.Response:
                if static_json is not None:
                    return httpx2.Response(static_status, json=static_json, headers=static_headers)
                return httpx2.Response(static_status, text=static_text or "", headers=static_headers)

        self._routes.append((method.upper(), path, handler))

    def _matches(self, route_path: Union[str, Pattern[str]], request_path: str) -> bool:
        if isinstance(route_path, str):
            return route_path == request_path
        return route_path.search(request_path) is not None

    def handle(self, request: httpx2.Request) -> httpx2.Response:
        with self._lock:
            self._requests.append(request)
        for method, path, handler in reversed(self._routes):
            if request.method == method and self._matches(path, request.url.path):
                return handler(request)
        return httpx2.Response(404, text=f"no route for {request.method} {request.url.path}")

    def transport(self) -> httpx2.MockTransport:
        return httpx2.MockTransport(self.handle)

    def client(self, **kwargs: Any) -> httpx2.Client:
        kwargs.setdefault("follow_redirects", True)
        return httpx2.Client(transport=self.transport(), **kwargs)

    def async_client(self, **kwargs: Any) -> httpx2.AsyncClient:
        kwargs.setdefault("follow_redirects", True)
        return httpx2.AsyncClient(transport=self.transport(), **kwargs)

    def requests(self, method: Optional[str] = None, path: Optional[str] = None) -> List[httpx2.Request]:
        """Recorded requests, optionally filtered by method and exact path."""
        with self._lock:
            recorded = list(self._requests)
        if method is not None:
            recorded = [r for r in recorded if r.method == method.upper()]
        if path is not None:
            recorded = [r for r in recorded if r.url.path == path]
        return recorded

    @property
    def last_request(self) -> httpx2.Request:
        with self._lock:
            return self._requests[-1]

    def reset_requests(self) -> None:
        with self._lock:
            self._requests.clear()


def json_response(status: int, body: Any, headers: Optional[Dict[str, str]] = None) -> httpx2.Response:
    return httpx2.Response(status, text=_json.dumps(body), headers=headers or {})


# Compiled catch-all pattern useful for fixtures.
MATCH_ALL = re.compile(r".*")
