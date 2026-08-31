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
import asyncio
import uuid

import httpx2
import pytest

import trino.auth
import trino.exceptions
from tests.unit.mock_http import MockTrinoServer
from tests.unit.oauth_test_utils import _get_token_requests
from tests.unit.oauth_test_utils import _post_statement_requests
from tests.unit.oauth_test_utils import GetTokenCallback
from tests.unit.oauth_test_utils import PostStatementCallback
from tests.unit.oauth_test_utils import REDIRECT_RESOURCE
from tests.unit.oauth_test_utils import RedirectHandler
from tests.unit.oauth_test_utils import TOKEN_PATH
from tests.unit.oauth_test_utils import TOKEN_RESOURCE
from trino import constants
from trino.aio.client import _AsyncRequestHeartbeat
from trino.aio.client import AsyncTrinoRequest
from trino.client import ClientSession


async def test_request_headers_are_sent(trino_server):
    req = AsyncTrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            client_tags=["tag1", "tag2"],
        ),
        http_session=trino_server.async_client(),
    )

    await req.post("SELECT 1")
    headers = trino_server.last_request.headers
    assert headers[constants.HEADER_USER] == "test_user"
    assert headers[constants.HEADER_CLIENT_TAGS] == "tag1,tag2"
    assert headers[constants.HEADER_CONTENT_TYPE] == constants.CONTENT_TYPE_TEXT_UTF8
    # None-valued headers (like the unset transaction id) are not sent
    assert constants.HEADER_TRANSACTION not in headers

    await req.get(req.statement_url)
    headers = trino_server.last_request.headers
    assert headers[constants.HEADER_USER] == "test_user"
    assert constants.HEADER_CONTENT_TYPE not in headers

    await req.aclose()


async def test_5XX_error_retry():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx2.Response(502)

    attempts = 3
    req = AsyncTrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test"),
        http_session=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)),
        max_attempts=attempts,
    )

    await req.post("SELECT 1")
    assert len(calls) == attempts

    await req.get(req.statement_url)
    assert len(calls) == 2 * attempts

    await req.aclose()


async def test_transport_error_retry():
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx2.ConnectError("connection refused", request=request)

    attempts = 3
    req = AsyncTrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test"),
        http_session=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)),
        max_attempts=attempts,
    )

    with pytest.raises(httpx2.ConnectError):
        await req.post("SELECT 1")
    assert len(calls) == attempts

    await req.aclose()


async def test_request_timeout_is_propagated_to_transport():
    seen = []

    def record_timeout(request):
        seen.append(request.extensions.get("timeout"))
        return httpx2.Response(200, text="ok")

    req = AsyncTrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test"),
        http_session=httpx2.AsyncClient(transport=httpx2.MockTransport(record_timeout)),
        max_attempts=1,
        request_timeout=(0.1, 0.2),
    )

    await req.post("SELECT 1")
    assert seen == [{"connect": 0.1, "read": 0.2, "write": None, "pool": None}]

    await req.aclose()


async def test_oauth2_authentication_flow(sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    server = MockTrinoServer()

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    server.register("POST", constants.URL_STATEMENT_PATH, post_statement_callback)

    get_token_callback = GetTokenCallback(token_server, token, attempts=2)
    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", get_token_callback)

    redirect_handler = RedirectHandler()
    auth = trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler)

    request = AsyncTrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        http_session=server.async_client(),
        auth=auth)
    response = await request.post("select 1")

    assert response.request.headers['Authorization'] == f"Bearer {token}"
    assert redirect_handler.redirect_server == redirect_server
    assert get_token_callback.attempts == 0
    assert len(_post_statement_requests(server)) == 2
    assert len(_get_token_requests(server, challenge_id)) == 2

    # The cached token is reused on the next request without a new challenge.
    response = await request.post("select 2")
    assert response.status_code == 200
    assert len(_post_statement_requests(server)) == 3

    await request.aclose()


async def test_heartbeat_sends_head_requests():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx2.Response(200, text="")

    req = AsyncTrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test"),
        http_session=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)),
    )
    req._next_uri = "http://coordinator:8080/v1/statement/x/1"

    async with _AsyncRequestHeartbeat(req, interval=0.01):
        await asyncio.sleep(0.1)

    assert len(calls) >= 1
    assert all(request.method == "HEAD" for request in calls)

    await req.aclose()


async def test_heartbeat_stops_when_unsupported():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx2.Response(405, text="")

    req = AsyncTrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test"),
        http_session=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)),
    )
    req._next_uri = "http://coordinator:8080/v1/statement/x/1"

    async with _AsyncRequestHeartbeat(req, interval=0.01):
        await asyncio.sleep(0.1)

    assert len(calls) == 1

    await req.aclose()
