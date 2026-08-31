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
import base64
import threading
import urllib
import uuid
from typing import Dict
from typing import Optional
from unittest import TestCase
from zoneinfo import ZoneInfoNotFoundError

import httpx2
import keyring
try:
    import orjson as json
except ImportError:
    import json
import pytest
from tzlocal import get_localzone_name  # type: ignore

import trino.exceptions
from tests.unit.mock_http import MockTrinoServer
from tests.unit.oauth_test_utils import _get_token_requests
from tests.unit.oauth_test_utils import _post_statement_requests
from tests.unit.oauth_test_utils import GetTokenCallback
from tests.unit.oauth_test_utils import MultithreadedTokenServer
from tests.unit.oauth_test_utils import PostStatementCallback
from tests.unit.oauth_test_utils import REDIRECT_RESOURCE
from tests.unit.oauth_test_utils import RedirectHandler
from tests.unit.oauth_test_utils import RedirectHandlerWithException
from tests.unit.oauth_test_utils import TOKEN_PATH
from tests.unit.oauth_test_utils import TOKEN_RESOURCE
from trino import __version__
from trino import constants
from trino.auth import _OAuth2KeyRingTokenCache
from trino.auth import _OAuth2TokenBearer
from trino.auth import GSSAPIAuthentication
from trino.auth import KerberosAuthentication
from trino.client import _DelayExponential
from trino.client import _retry_with
from trino.client import _RetryWithExponentialBackoff
from trino.client import ClientSession
from trino.client import CompressedQueryDataDecoderFactory
from trino.client import TrinoQuery
from trino.client import TrinoRequest
from trino.client import TrinoResult
from trino.exceptions import SPNEGOExchangeError

try:
    import gssapi  # noqa: F401
    _GSSAPI_AVAILABLE = True
except ImportError:
    _GSSAPI_AVAILABLE = False

requires_gssapi = pytest.mark.skipif(
    not _GSSAPI_AVAILABLE,
    reason="gssapi is not installed",
)


def _json_response(data, status_code=200, headers=None):
    body = json.dumps(data)
    if isinstance(body, str):
        body = body.encode("utf-8")
    return httpx2.Response(status_code, content=body, headers=headers or {})


def test_trino_initial_request(sample_post_response_data):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = _json_response(sample_post_response_data)
    status = req.process(http_resp)

    assert status.next_uri == sample_post_response_data["nextUri"]
    assert status.id == sample_post_response_data["id"]


def test_request_headers(trino_server):
    catalog = "test_catalog"
    schema = "test_schema"
    user = "test_user"
    authorization_user = "test_authorization_user"
    source = "test_source"
    timezone = "Europe/Brussels"
    accept_encoding_header = "accept-encoding"
    accept_encoding_value = "identity,deflate,gzip"
    client_info_header = constants.HEADER_CLIENT_INFO
    client_info_value = "some_client_info"
    encoding = "json+zstd"

    with pytest.deprecated_call():
        req = TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(
                user=user,
                authorization_user=authorization_user,
                source=source,
                catalog=catalog,
                schema=schema,
                timezone=timezone,
                encoding=encoding,
                headers={
                    accept_encoding_header: accept_encoding_value,
                    client_info_header: client_info_value,
                },
                roles={
                    "hive": "ALL",
                    "system": "analyst",
                    "catalog1": "NONE",
                    # ensure backwards compatibility
                    "catalog2": "ROLE{catalog2_role}",
                }
            ),
            http_scheme="http",
            http_session=trino_server.client(),
        )

    def assert_headers(headers):
        assert headers[constants.HEADER_CATALOG] == catalog
        assert headers[constants.HEADER_SCHEMA] == schema
        assert headers[constants.HEADER_SOURCE] == source
        assert headers[constants.HEADER_ORIGINAL_USER] == user
        assert headers[constants.HEADER_USER] == authorization_user
        assert headers[constants.HEADER_SESSION] == ""
        # None-valued headers (like the unset transaction id) are not sent
        assert constants.HEADER_TRANSACTION not in headers
        assert headers[constants.HEADER_TIMEZONE] == timezone
        assert headers[constants.HEADER_CLIENT_CAPABILITIES] == constants.CLIENT_CAPABILITIES
        assert headers[accept_encoding_header] == accept_encoding_value
        assert headers[client_info_header] == client_info_value
        assert headers[constants.HEADER_ROLE] == (
            "hive=ALL,"
            "system=" + urllib.parse.quote("ROLE{analyst}") + ","
            "catalog1=NONE,"
            "catalog2=" + urllib.parse.quote("ROLE{catalog2_role}")
        )
        assert headers["User-Agent"] == f"{constants.CLIENT_NAME}/{__version__}"
        assert headers[constants.HEADER_ENCODING] == encoding

    req.post("URL")
    post_headers = trino_server.last_request.headers
    assert post_headers[constants.HEADER_CONTENT_TYPE] == constants.CONTENT_TYPE_TEXT_UTF8
    assert_headers(post_headers)

    req.get(req.statement_url)
    get_headers = trino_server.last_request.headers
    assert constants.HEADER_CONTENT_TYPE not in get_headers
    assert_headers(get_headers)


def test_post_sets_content_type_charset(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
        http_session=trino_server.client(),
    )

    req.post("SELECT 1")
    headers = trino_server.last_request.headers
    assert headers[constants.HEADER_CONTENT_TYPE] == constants.CONTENT_TYPE_TEXT_UTF8


def test_post_content_type_can_be_overridden(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
        http_session=trino_server.client(),
    )

    req.post("SELECT 1", additional_http_headers={constants.HEADER_CONTENT_TYPE: "application/xyz"})
    headers = trino_server.last_request.headers
    assert headers[constants.HEADER_CONTENT_TYPE] == "application/xyz"


def test_request_session_properties_headers(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            properties={
                "a": "1",
                "b": "2",
                "c": "more=v1,v2"
            }
        ),
        http_session=trino_server.client(),
    )

    def assert_headers(headers):
        assert headers[constants.HEADER_SESSION] == "a=1,b=2,c=more%3Dv1%2Cv2"

    req.post("URL")
    assert_headers(trino_server.last_request.headers)

    req.get(req.statement_url)
    assert_headers(trino_server.last_request.headers)


def test_additional_request_post_headers(trino_server):
    """
    Tests that the `TrinoRequest.post` function can take addtional headers
    and that it combines them with the existing ones to perform the request.
    """
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
        http_session=trino_server.client(),
    )

    sql = 'select 1'
    additional_headers = {
        'X-Trino-Fake-1': 'one',
        'X-Trino-Fake-2': 'two',
    }

    combined_headers = req.http_headers
    combined_headers.update(additional_headers)
    combined_headers.setdefault(constants.HEADER_CONTENT_TYPE, constants.CONTENT_TYPE_TEXT_UTF8)

    req.post(sql, additional_headers)

    # Validate that the post call was performed including the addtional headers
    sent_headers = trino_server.last_request.headers
    for key, value in combined_headers.items():
        if value is None:
            assert key not in sent_headers
        else:
            assert sent_headers[key] == value


def test_request_invalid_http_headers():
    with pytest.raises(ValueError) as value_error:
        TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(
                user="test",
                headers={constants.HEADER_USER: "invalid_header"},
            ),
        )
    assert str(value_error.value).startswith("cannot override reserved HTTP header")


def test_request_client_tags_headers(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            client_tags=["tag1", "tag2"]
        ),
        http_session=trino_server.client(),
    )

    def assert_headers(headers):
        assert headers[constants.HEADER_CLIENT_TAGS] == "tag1,tag2"

    req.post("URL")
    assert_headers(trino_server.last_request.headers)

    req.get(req.statement_url)
    assert_headers(trino_server.last_request.headers)


def test_request_client_tags_headers_no_client_tags(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
        ),
        http_session=trino_server.client(),
    )

    def assert_headers(headers):
        assert constants.HEADER_CLIENT_TAGS not in headers

    req.post("URL")
    assert_headers(trino_server.last_request.headers)

    req.get(req.statement_url)
    assert_headers(trino_server.last_request.headers)


def test_enabling_https_automatically_when_using_port_443(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_session=trino_server.client(),
    )

    req.post("SELECT 1")
    assert trino_server.last_request.url.scheme == constants.HTTPS


def test_https_scheme(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        http_session=trino_server.client(),
    )

    req.post("SELECT 1")
    url = trino_server.last_request.url
    assert url.scheme == constants.HTTPS
    # httpx normalizes the default port away
    assert url.port in (None, constants.DEFAULT_TLS_PORT)


def test_http_scheme_with_port(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTP,
        http_session=trino_server.client(),
    )

    req.post("SELECT 1")
    url = trino_server.last_request.url
    assert url.scheme == constants.HTTP
    assert url.port == constants.DEFAULT_TLS_PORT


def test_request_timeout_is_propagated_to_transport():
    seen = []

    def record_timeout(request):
        seen.append(request.extensions.get("timeout"))
        return httpx2.Response(200, text="ok")

    url = "http://coordinator:8080" + constants.URL_STATEMENT_PATH

    for request_timeout, expected in [
        (0.1, {"connect": 0.1, "read": 0.1, "write": 0.1, "pool": 0.1}),
        ((0.1, 0.2), {"connect": 0.1, "read": 0.2, "write": None, "pool": None}),
    ]:
        seen.clear()
        req = TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(
                user="test",
            ),
            http_scheme="http",
            http_session=httpx2.Client(transport=httpx2.MockTransport(record_timeout)),
            max_attempts=1,
            request_timeout=request_timeout,
        )

        req.get(url)
        req.post("select 1")
        assert seen == [expected, expected]


def test_request_timeout():
    def raise_timeout(request):
        raise httpx2.ReadTimeout("timed out", request=request)

    url = "http://coordinator:8080" + constants.URL_STATEMENT_PATH

    # timeout without retry
    for request_timeout in [0.1, (0.1, 0.1)]:
        req = TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(
                user="test",
            ),
            http_scheme="http",
            http_session=httpx2.Client(transport=httpx2.MockTransport(raise_timeout)),
            max_attempts=1,
            request_timeout=request_timeout,
        )

        with pytest.raises(httpx2.TimeoutException):
            req.get(url)

        with pytest.raises(httpx2.TimeoutException):
            req.post("select 1")


def test_redirects_are_followed(trino_server):
    query_path = constants.URL_STATEMENT_PATH + "/redirected"
    trino_server.register(
        "GET", constants.URL_STATEMENT_PATH,
        status=301, headers={"Location": f"https://coordinator{query_path}"}, text="")
    trino_server.register("GET", query_path, json={"redirected": True})

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        http_session=trino_server.client(),
    )

    response = req.get(req.statement_url)
    assert response.status_code == 200
    assert response.url.path == query_path
    assert len(trino_server.requests(method="GET")) == 2


@pytest.mark.parametrize("attempts", [1, 3, 5])
def test_oauth2_authentication_flow(attempts, sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    server = MockTrinoServer()

    # bind post statement
    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    server.register("POST", constants.URL_STATEMENT_PATH, post_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token, attempts)
    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", get_token_callback)

    redirect_handler = RedirectHandler()

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        http_session=server.client(),
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler))
    response = request.post("select 1")

    assert response.request.headers['Authorization'] == f"Bearer {token}"
    assert redirect_handler.redirect_server == redirect_server
    assert get_token_callback.attempts == 0
    assert len(_post_statement_requests(server)) == 2
    assert len(_get_token_requests(server, challenge_id)) == attempts


def test_oauth2_refresh_token_flow(sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    server = MockTrinoServer()

    post_statement_callback = PostStatementCallback(None, token_server, [token], sample_post_response_data)
    server.register("POST", constants.URL_STATEMENT_PATH, post_statement_callback)

    get_token_callback = GetTokenCallback(token_server, token)
    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", get_token_callback)

    redirect_handler = RedirectHandlerWithException(
        trino.exceptions.TrinoAuthError(
            "Do not use redirect handler when there is no redirect_uri in the response"))

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        http_session=server.client(),
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler))

    response = request.post("select 1")

    assert response.request.headers['Authorization'] == f"Bearer {token}"
    assert get_token_callback.attempts == 0
    assert len(_post_statement_requests(server)) == 2


@pytest.mark.parametrize("attempts", [6, 10])
def test_oauth2_exceed_max_attempts(attempts, sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    server = MockTrinoServer()

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    server.register("POST", constants.URL_STATEMENT_PATH, post_statement_callback)

    get_token_callback = GetTokenCallback(token_server, token, attempts)
    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", get_token_callback)

    redirect_handler = RedirectHandler()

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        http_session=server.client(),
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler))
    with pytest.raises(trino.exceptions.TrinoAuthError) as exp:
        request.post("select 1")

    assert str(exp.value) == "Exceeded max attempts while getting the token"
    assert redirect_handler.redirect_server == redirect_server
    assert get_token_callback.attempts == attempts - _OAuth2TokenBearer.MAX_OAUTH_ATTEMPTS
    assert len(_post_statement_requests(server)) == 1
    assert len(_get_token_requests(server, challenge_id)) == _OAuth2TokenBearer.MAX_OAUTH_ATTEMPTS


@pytest.mark.parametrize("header,error", [
    ("", "Error: header WWW-Authenticate not available in the response."),
    ('Bearer"', 'Error: header info didn\'t have x_token_server'),
    ('x_redirect_server="redirect_server", x_token_server="token_server"', 'Error: header info didn\'t match x_redirect_server="redirect_server", x_token_server="token_server"'),  # noqa: E501
    ('Bearer x_redirect_server="redirect_server"', 'Error: header info didn\'t have x_token_server'),
])
def test_oauth2_authentication_missing_headers(header, error):
    server = MockTrinoServer()
    server.register(
        "POST", constants.URL_STATEMENT_PATH,
        status=401, headers={'WWW-Authenticate': header}, text="")

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        http_session=server.client(),
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=RedirectHandler()))

    with pytest.raises(trino.exceptions.TrinoAuthError) as exp:
        request.post("select 1")

    assert str(exp.value) == error


@pytest.mark.parametrize("header", [
    'Bearer x_redirect_server="{redirect_server}", x_token_server="{token_server}", additional_challenge',
    'Bearer x_redirect_server="{redirect_server}", x_token_server="{token_server}", additional_challenge="value"',
    'Bearer x_token_server="{token_server}", x_redirect_server="{redirect_server}"',
    'Basic realm="Trino", Bearer x_redirect_server="{redirect_server}", x_token_server="{token_server}"',
    'Bearer x_redirect_server="{redirect_server}", x_token_server="{token_server}", Basic realm="Trino"',
    'Basic realm="Trino", Bearer realm="Trino", token_type="JWT", Bearer x_redirect_server="{redirect_server}", '
    'x_token_server="{token_server}"'
    'Bearer x_redirect_server="{redirect_server}",x_token_server="{token_server}",additional_challenge',
])
def test_oauth2_header_parsing(header, sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}?role=test"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    def post_statement(request):
        authorization = request.headers.get("Authorization")
        if authorization and authorization.replace("Bearer ", "") in token:
            return _json_response(sample_post_response_data)
        return httpx2.Response(
            401,
            headers={'Www-Authenticate': header.format(redirect_server=redirect_server, token_server=token_server),
                     'Basic realm': '"Trino"'},
            text="")

    server = MockTrinoServer()
    server.register("POST", constants.URL_STATEMENT_PATH, post_statement)

    get_token_callback = GetTokenCallback(token_server, token)
    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", get_token_callback)

    redirect_handler = RedirectHandler()

    response = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        http_session=server.client(),
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler)
    ).post("select 1")

    assert response.request.headers['Authorization'] == f"Bearer {token}"
    assert redirect_handler.redirect_server == redirect_server
    assert get_token_callback.attempts == 0
    assert len(_post_statement_requests(server)) == 2
    assert len(_get_token_requests(server, challenge_id)) == 1


@pytest.mark.parametrize("http_status", [400, 401, 500])
def test_oauth2_authentication_fail_token_server(http_status, sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    server = MockTrinoServer()

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    server.register("POST", constants.URL_STATEMENT_PATH, post_statement_callback)

    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", status=http_status, text="error")

    redirect_handler = RedirectHandler()

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        http_session=server.client(),
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler))

    with pytest.raises(trino.exceptions.TrinoAuthError) as exp:
        request.post("select 1")

    assert redirect_handler.redirect_server == redirect_server
    assert str(exp.value) == f"Error while getting the token response status code: {http_status}, body: error"
    assert len(_post_statement_requests(server)) == 1
    assert len(_get_token_requests(server, challenge_id)) == 1


def test_multithreaded_oauth2_authentication_flow(sample_post_response_data):
    redirect_handler = RedirectHandler()
    auth = trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler)

    server = MockTrinoServer()
    token_server = MultithreadedTokenServer(server, sample_post_response_data)

    class RunningThread(threading.Thread):
        # Serialize the posts so that exactly one challenge is issued: the
        # first post completes its whole OAuth2 flow before any other thread
        # sends an unauthenticated request.
        lock = threading.Lock()

        def __init__(self):
            super().__init__()
            self.token = None

        def run(self) -> None:
            request = TrinoRequest(
                host="coordinator",
                port=constants.DEFAULT_TLS_PORT,
                client_session=ClientSession(
                    user="test",
                ),
                http_scheme=constants.HTTPS,
                http_session=server.client(),
                auth=auth)
            for i in range(10):
                with RunningThread.lock:
                    response = request.post("select 1")
                self.token = response.request.headers["Authorization"].replace("Bearer ", "")

    threads = [RunningThread(), RunningThread(), RunningThread()]

    # run and join all threads
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # should issue only 1 token and each thread should reuse it
    assert len(token_server.tokens) == 1
    for thread in threads:
        assert thread.token in token_server.tokens

    # should start only 1 challenge
    assert len(token_server.challenges.keys()) == 1
    for challenge_id, challenge in token_server.challenges.items():
        assert f"{REDIRECT_RESOURCE}/{challenge_id}" in redirect_handler.redirect_server
        assert challenge.attempts == 0
        assert len(_get_token_requests(server, challenge_id)) == 1
    # 3 threads * (10 POST /statement each + 1 replied request by authentication)
    assert len(_post_statement_requests(server)) == 31


def test_trino_fetch_request(sample_get_response_data):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = _json_response(sample_get_response_data)
    status = req.process(http_resp)

    assert status.next_uri == sample_get_response_data["nextUri"]
    assert status.id == sample_get_response_data["id"]
    assert status.rows == sample_get_response_data["data"]


def test_trino_fetch_request_data_none(sample_get_response_data_none):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = _json_response(sample_get_response_data_none)
    status = req.process(http_resp)

    assert status.next_uri == sample_get_response_data_none["nextUri"]
    assert status.id == sample_get_response_data_none["id"]
    assert status.rows == []


def test_trino_fetch_error(sample_get_error_response_data):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = _json_response(sample_get_error_response_data)
    with pytest.raises(trino.exceptions.TrinoUserError) as exception_info:
        req.process(http_resp)
    error = exception_info.value
    assert error.error_code == 1
    assert error.error_name == "SYNTAX_ERROR"
    assert error.error_type == "USER_ERROR"
    assert error.error_exception == "io.trino.spi.TrinoException"
    assert "stack" in error.failure_info
    assert len(error.failure_info["stack"]) == 36
    assert "suppressed" in error.failure_info
    assert (
        error.message
        == "line 1:15: Schema must be specified when session schema is not set"
    )
    assert error.error_location == (1, 15)
    assert error.query_id == "20210817_140827_00000_arvdv"


@pytest.mark.parametrize(
    "error_code, error_type, error_message",
    [
        (503, trino.exceptions.Http503Error, "service unavailable"),
        (504, trino.exceptions.Http504Error, "gateway timeout"),
        (404, trino.exceptions.HttpError, "error 404"),
    ],
)
def test_trino_connection_error(error_code, error_type, error_message):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = httpx2.Response(error_code)
    with pytest.raises(error_type) as error:
        req.process(http_resp)
    assert error_message in str(error)


def test_trino_process_empty_200_response_error():
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    http_resp = httpx2.Response(200, content=b"")
    with pytest.raises(trino.exceptions.TrinoConnectionError) as error:
        req.process(http_resp)
    assert "received empty response from server (status 200)" in str(error.value)


def test_extra_credential(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
            extra_credential=[("a.username", "foo"), ("b.password", "bar")],
        ),
        http_session=trino_server.client(),
    )

    req.post("SELECT 1")
    headers = trino_server.last_request.headers
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "a.username=foo, b.password=bar"


def test_extra_credential_key_with_illegal_chars():
    with pytest.raises(ValueError) as e_info:
        TrinoRequest(
            host="coordinator",
            port=constants.DEFAULT_TLS_PORT,
            client_session=ClientSession(
                user="test",
                extra_credential=[("a=b", "")],
            ),
        )

    assert str(e_info.value) == "whitespace or '=' are disallowed in extra credential 'a=b'"


def test_extra_credential_key_non_ascii():
    with pytest.raises(ValueError) as e_info:
        TrinoRequest(
            host="coordinator",
            port=constants.DEFAULT_TLS_PORT,
            client_session=ClientSession(
                user="test",
                extra_credential=[("的", "")],
            ),
        )

    assert str(e_info.value) == "only ASCII characters are allowed in extra credential '的'"


def test_extra_credential_value_encoding(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
            extra_credential=[("foo", "bar 的")],
        ),
        http_session=trino_server.client(),
    )

    req.post("SELECT 1")
    headers = trino_server.last_request.headers
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "foo=bar+%E7%9A%84"


def test_extra_credential_value_object(trino_server):
    class TestCredential:
        value = "initial"

        def __str__(self):
            return self.value

    credential = TestCredential()

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
            extra_credential=[("foo", credential)]
        ),
        http_session=trino_server.client(),
    )

    req.post("SELECT 1")
    headers = trino_server.last_request.headers
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "foo=initial"

    # Make a second request, assert that credential has changed
    credential.value = "changed"
    req.post("SELECT 1")
    headers = trino_server.last_request.headers
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "foo=changed"


class RetryRecorder:
    """Transport handler counting how often it was hit, raising a canned error
    or returning a freshly built response on every call."""

    def __init__(self, error=None, result=None):
        self._retry_count = 0
        self._error = error
        self._result = result

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self._retry_count += 1
        if self._error is not None:
            raise self._error
        return self._result()

    @property
    def retry_count(self):
        return self._retry_count


@pytest.mark.parametrize("auth_class", [
    pytest.param(KerberosAuthentication, marks=requires_gssapi),
    pytest.param(GSSAPIAuthentication, marks=requires_gssapi),
])
def test_authentication_fail_retry(auth_class):
    recorder = RetryRecorder(error=SPNEGOExchangeError("token exchange failed"))
    auth = auth_class()

    attempts = 3
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        http_session=TrinoRequest.create_http_client(
            auth=auth, transport=httpx2.MockTransport(recorder)),
        auth=auth,
        max_attempts=attempts,
    )

    with pytest.raises(SPNEGOExchangeError):
        req.post("URL")
    assert recorder.retry_count == attempts

    with pytest.raises(SPNEGOExchangeError):
        req.get(req.statement_url)
    assert recorder.retry_count == 2 * attempts


@pytest.mark.parametrize("status_code, attempts", [
    (502, 3),
    (503, 3),
    (504, 3),
])
def test_5XX_error_retry(status_code, attempts):
    recorder = RetryRecorder(result=lambda: httpx2.Response(status_code))

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        http_session=httpx2.Client(transport=httpx2.MockTransport(recorder)),
        max_attempts=attempts
    )

    req.post("URL")
    assert recorder.retry_count == attempts

    req.get(req.statement_url)
    assert recorder.retry_count == 2 * attempts


def test_429_error_retry():
    recorder = RetryRecorder(result=lambda: httpx2.Response(429, headers={"Retry-After": "1"}))

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        http_session=httpx2.Client(transport=httpx2.MockTransport(recorder)),
        max_attempts=3
    )

    req.post("URL")
    assert recorder.retry_count == 3

    req.get(req.statement_url)
    assert recorder.retry_count == 6


def test_empty_200_response_retry():
    recorder = RetryRecorder(result=lambda: httpx2.Response(200, content=b""))

    attempts = 3
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test"),
        http_session=httpx2.Client(transport=httpx2.MockTransport(recorder)),
        max_attempts=attempts,
    )

    req.post("SELECT 1")
    assert recorder.retry_count == attempts

    req.get(req.statement_url)
    assert recorder.retry_count == 2 * attempts


@pytest.mark.parametrize("status_code", [
    501
])
def test_error_no_retry(status_code):
    recorder = RetryRecorder(result=lambda: httpx2.Response(status_code))

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        http_session=httpx2.Client(transport=httpx2.MockTransport(recorder)),
        max_attempts=3,
    )

    req.post("URL")
    assert recorder.retry_count == 1

    req.get(req.statement_url)
    assert recorder.retry_count == 2


def test_trino_query_response_headers(sample_get_response_data):
    """
    Validates that the `TrinoQuery.execute` function can take addtional headers
    that are pass the the provided request instance post function call and it
    returns a `TrinoResult` instance.
    """
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
            source="test",
            catalog="test",
            schema="test",
            properties={},
        ),
        http_scheme="http",
    )

    sql = 'execute my_stament using 1, 2, 3'
    additional_headers = {
        constants.HEADER_PREPARED_STATEMENT: 'my_statement=added_prepare_statement_header',
        constants.HEADER_CLIENT_CAPABILITIES: 'PARAMETRIC_DATETIME,SESSION_AUTHORIZATION'
    }

    # Replace the post function with a hand-written fake to avoid making
    # requests, and to validate it was called with the right arguments.
    calls = []
    fake_response = _json_response(
        sample_get_response_data,
        headers={
            'X-Trino-Fake-1': 'one',
            'X-Trino-Fake-2': 'two',
        })

    def fake_post(sql, additional_http_headers=None):
        calls.append((sql, additional_http_headers))
        return fake_response

    req.post = fake_post

    query = TrinoQuery(
        request=req,
        query=sql
    )
    result = query.execute(additional_http_headers=additional_headers)

    # Validate the the post function was called with the right argguments
    assert calls == [(sql, additional_headers)]

    # Validate the result is an instance of TrinoResult
    assert isinstance(result, TrinoResult)


def test_stats_callback_cannot_mutate_query_stats():
    received = []

    def stats_callback(stats):
        received.append(stats)
        stats["state"] = "MUTATED"
        stats["rootStage"]["subStages"][0]["stageId"] = "999"

    query = TrinoQuery(
        request=TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(user="test"),
            http_scheme="http",
        ),
        query="SELECT 1",
        stats_callback=stats_callback,
    )
    query._stats = {
        "queryId": "q1",
        "state": "RUNNING",
        "rootStage": {"stageId": "0", "subStages": [{"stageId": "1"}]},
    }

    query._report_stats()

    assert received == [{
        "queryId": "q1",
        "state": "MUTATED",
        "rootStage": {"stageId": "0", "subStages": [{"stageId": "999"}]},
    }]
    assert query.stats == {
        "queryId": "q1",
        "state": "RUNNING",
        "rootStage": {"stageId": "0", "subStages": [{"stageId": "1"}]},
    }


def test_delay_exponential_without_jitter():
    max_delay = 1200.0
    get_delay = _DelayExponential(base=5, jitter=False, max_delay=max_delay)
    results = [
        10.0,
        20.0,
        40.0,
        80.0,
        160.0,
        320.0,
        640.0,
        max_delay,  # rather than 1280.0
        max_delay,  # rather than 2560.0
    ]
    for i, result in enumerate(results, start=1):
        assert get_delay(i) == result


def test_delay_exponential_with_jitter():
    max_delay = 120.0
    get_delay = _DelayExponential(base=10, jitter=False, max_delay=max_delay)
    for i in range(10):
        assert get_delay(i) <= max_delay


class SomeException(Exception):
    pass


def test_retry_with():
    max_attempts = 3
    with_retry = _retry_with(
        handle_retry=_RetryWithExponentialBackoff(),
        handled_exceptions=[SomeException],
        conditions={},
        max_attempts=max_attempts,
    )

    class FailerUntil:
        def __init__(self, until=1):
            self.attempt = 0
            self._until = until

        def __call__(self):
            self.attempt += 1
            if self.attempt > self._until:
                return
            raise SomeException(self.attempt)

    with_retry(FailerUntil(2).__call__)()
    with pytest.raises(SomeException):
        with_retry(FailerUntil(3).__call__)()


def assert_headers_with_roles(headers: Dict[str, str], roles: Optional[str]):
    if roles is None:
        assert constants.HEADER_ROLE not in headers
    else:
        assert headers[constants.HEADER_ROLE] == roles
    assert headers[constants.HEADER_USER] == "test_user"


def test_request_headers_role_hive_all(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            roles={"hive": "ALL"}
        ),
        http_session=trino_server.client(),
    )

    req.post("URL")
    assert_headers_with_roles(trino_server.last_request.headers, "hive=ALL")

    req.get(req.statement_url)
    assert_headers_with_roles(trino_server.last_request.headers, "hive=ALL")


def test_request_headers_role_admin(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            roles={"system": "admin"}
        ),
        http_session=trino_server.client(),
    )
    roles = "system=" + urllib.parse.quote("ROLE{admin}")

    req.post("URL")
    assert_headers_with_roles(trino_server.last_request.headers, roles)

    req.get(req.statement_url)
    assert_headers_with_roles(trino_server.last_request.headers, roles)


def test_request_headers_role_empty(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            roles=None,
        ),
        http_session=trino_server.client(),
    )

    req.post("URL")
    assert_headers_with_roles(trino_server.last_request.headers, None)

    req.get(req.statement_url)
    assert_headers_with_roles(trino_server.last_request.headers, None)


def assert_headers_timezone(headers: Dict[str, str], timezone: str):
    assert headers[constants.HEADER_TIMEZONE] == timezone


def test_request_headers_with_timezone(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            timezone="Europe/Brussels"
        ),
        http_session=trino_server.client(),
    )

    req.post("URL")
    assert_headers_timezone(trino_server.last_request.headers, "Europe/Brussels")

    req.get(req.statement_url)
    assert_headers_timezone(trino_server.last_request.headers, "Europe/Brussels")


def test_request_headers_without_timezone(trino_server):
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
        ),
        http_session=trino_server.client(),
    )
    localzone = get_localzone_name()

    req.post("URL")
    assert_headers_timezone(trino_server.last_request.headers, localzone)

    req.get(req.statement_url)
    assert_headers_timezone(trino_server.last_request.headers, localzone)


def test_request_with_invalid_timezone():
    with pytest.raises(ZoneInfoNotFoundError) as zinfo_error:
        TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(
                user="test_user",
                timezone="INVALID_TIMEZONE"
            ),
        )
    assert str(zinfo_error.value).startswith("'No time zone found with key")


class TestShardedPassword(TestCase):
    def test_store_short_password(self):
        # set the keyring to mock class
        keyring.set_keyring(MockKeyring())

        host = "trino.com"
        short_password = "x" * 10

        cache = _OAuth2KeyRingTokenCache()
        cache.store_token_to_cache(host, short_password)

        retrieved_password = cache.get_token_from_cache(host)
        self.assertEqual(short_password, retrieved_password)

    def test_store_long_password(self):
        # set the keyring to mock class
        keyring.set_keyring(MockKeyring())

        host = "trino.com"
        long_password = "x" * 3000

        cache = _OAuth2KeyRingTokenCache()
        cache.store_token_to_cache(host, long_password)

        retrieved_password = cache.get_token_from_cache(host)
        self.assertEqual(long_password, retrieved_password)


class MockKeyring(keyring.backend.KeyringBackend):
    def __init__(self):
        self.file_location = self._generate_test_root_dir()

    @staticmethod
    def _generate_test_root_dir():
        import tempfile

        return tempfile.mkdtemp(prefix="trino-python-client-unit-test-")

    def file_path(self, servicename, username):
        from os.path import join

        file_location = self.file_location
        file_name = f"{servicename}_{username}.txt"
        return join(file_location, file_name)

    def set_password(self, servicename, username, password):
        file_path = self.file_path(servicename, username)

        with open(file_path, "w") as file:
            file.write(password)

    def get_password(self, servicename, username):
        import os

        file_path = self.file_path(servicename, username)
        if not os.path.exists(file_path):
            return None

        with open(file_path, "r") as file:
            password = file.read()

        return password

    def delete_password(self, servicename, username):
        import os

        file_path = self.file_path(servicename, username)
        if not os.path.exists(file_path):
            return None

        os.remove(file_path)


def test_trino_request_headers_encoding_default_behavior(monkeypatch):
    session = ClientSession(user="test", encoding=None)

    # Case 1: Both available -> No header
    monkeypatch.setattr("trino._protocol.CODECS_UNAVAILABLE", {})
    req = TrinoRequest("host", 8080, session)
    assert constants.HEADER_ENCODING not in req.http_headers

    # Case 2: Zstd missing -> Header set with json+lz4,json
    monkeypatch.setattr("trino._protocol.CODECS_UNAVAILABLE", {"zstd": "Not installed"})
    req = TrinoRequest("host", 8080, session)
    assert req.http_headers[constants.HEADER_ENCODING] == "json+lz4,json"

    # Case 3: Lz4 missing -> Header set with json+zstd,json
    monkeypatch.setattr("trino._protocol.CODECS_UNAVAILABLE", {"lz4": "Not installed"})
    req = TrinoRequest("host", 8080, session)
    assert req.http_headers[constants.HEADER_ENCODING] == "json+zstd,json"

    # Case 4: Both missing -> Header set with json
    monkeypatch.setattr("trino._protocol.CODECS_UNAVAILABLE", {"lz4": "Not installed", "zstd": "Not installed"})
    req = TrinoRequest("host", 8080, session)
    assert req.http_headers[constants.HEADER_ENCODING] == "json"


def test_decoder_factory_raises_with_message_on_missing_zstd(monkeypatch):
    factory = CompressedQueryDataDecoderFactory(object())
    error_message = "No module named 'zstandard'"
    monkeypatch.setattr("trino._protocol.CODECS_UNAVAILABLE", {"zstd": error_message})
    with pytest.raises(
        ValueError,
        match=f"zstd is not installed so json\\+zstd encoding is not supported: {error_message}"
    ):
        factory.create("json+zstd")


def test_decoder_factory_raises_with_message_on_missing_lz4(monkeypatch):
    factory = CompressedQueryDataDecoderFactory(object())
    error_message = "No module named 'lz4.block'"
    monkeypatch.setattr("trino._protocol.CODECS_UNAVAILABLE", {"lz4": error_message})
    with pytest.raises(
        ValueError,
        match=f"lz4 is not installed so json\\+lz4 encoding is not supported: {error_message}"
    ):
        factory.create("json+lz4")


class _FinishedQuery:
    """Query stub that is already finished. All rows come from the initial batch."""
    finished = True

    def fetch(self):
        return []


@pytest.mark.parametrize("consecutive_failures", (1, 2, 3))
def test_trino_result_resumes_after_transient_error_in_rows_iterator(consecutive_failures):
    class FlakyIterator:
        """Fails a given number of times at the third row, succeeds when retried."""
        def __init__(self, failures):
            self._rows = iter([[1], [2], [3]])
            self._served = 0
            self._remaining_failures = failures

        def __iter__(self):
            return self

        def __next__(self):
            if self._served == 2 and self._remaining_failures > 0:
                self._remaining_failures -= 1
                raise IOError("segment download failed")
            self._served += 1
            return next(self._rows)

    result = TrinoResult(_FinishedQuery(), FlakyIterator(consecutive_failures))
    it = iter(result)
    assert next(it) == [1]
    assert next(it) == [2]
    # Each retry surfaces the error again until the underlying iterator recovers
    for _ in range(consecutive_failures):
        with pytest.raises(IOError):
            next(it)
    # The iterator stays usable and resumes where the failure happened
    assert next(it) == [3]
    with pytest.raises(StopIteration):
        next(it)
    assert result.rownumber == 3


def test_trino_result_reraises_persistent_error_instead_of_stopping():
    class FailingIterator:
        def __init__(self):
            self._count = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self._count >= 3:
                raise IOError("segment download failed")
            self._count += 1
            return [self._count]

    result = TrinoResult(_FinishedQuery(), FailingIterator())
    it = iter(result)
    assert [next(it), next(it), next(it)] == [[1], [2], [3]]
    # The error keeps surfacing instead of turning into StopIteration
    # which dbapi would report as a normally exhausted result set
    with pytest.raises(IOError):
        next(it)
    with pytest.raises(IOError):
        next(it)


def test_trino_result_resumes_after_transient_fetch_error():
    class FlakyQuery:
        def __init__(self):
            self.finished = False
            self._fetches = 0

        def fetch(self):
            self._fetches += 1
            if self._fetches == 1:
                raise IOError("connection reset")
            self.finished = True
            return [[2]]

    result = TrinoResult(FlakyQuery(), [[1]])
    it = iter(result)
    # The next batch is prefetched before the first row is served so the
    # fetch error surfaces before any rows
    with pytest.raises(IOError):
        next(it)
    assert next(it) == [1]
    assert next(it) == [2]
    with pytest.raises(StopIteration):
        next(it)


def test_execute_drains_spooled_update_query_with_trailing_page():
    """An update statement whose count row arrives over the spooling protocol still has a
    trailing page to drain. The rows are a lazy iterator at that point, so the drain loop
    must chain onto it rather than use list concatenation.
    """
    query_id = "20210817_140827_00000_arvdv"
    server_address = "https://coordinator"
    statement_path = constants.URL_STATEMENT_PATH

    def statement_uri(token):
        return f"{server_address}{statement_path}/{query_id}/{token}"

    columns = [{
        "name": "rows",
        "type": "bigint",
        "typeSignature": {"rawType": "bigint", "arguments": [], "typeArguments": []},
    }]
    # The update-count row is delivered as a base64 encoded inline segment.
    encoded_rows = base64.b64encode(b"[[3]]").decode("utf8")

    post_response = {
        "id": query_id,
        "nextUri": statement_uri(1),
        "infoUri": f"{server_address}/query.html?{query_id}",
        "stats": {"state": "QUEUED"},
    }
    spooled_update_response = {
        "id": query_id,
        "nextUri": statement_uri(2),
        "infoUri": f"{server_address}/query.html?{query_id}",
        "updateType": "INSERT",
        "updateCount": 3,
        "columns": columns,
        "data": {
            "encoding": "json",
            "segments": [{
                "type": "inline",
                "metadata": {"uncompressedSize": "5", "segmentSize": "5"},
                "data": encoded_rows,
            }],
        },
        "stats": {"state": "FINISHED"},
    }
    # Trailing page with no data. It only moves the query to a terminal state.
    final_response = {
        "id": query_id,
        "infoUri": f"{server_address}/query.html?{query_id}",
        "updateType": "INSERT",
        "updateCount": 3,
        "columns": columns,
        "stats": {"state": "FINISHED"},
    }

    server = MockTrinoServer()
    server.register("POST", statement_path, json=post_response)
    server.register("GET", f"{statement_path}/{query_id}/1", json=spooled_update_response)
    server.register("GET", f"{statement_path}/{query_id}/2", json=final_response)

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(user="test", encoding="json"),
        http_scheme=constants.HTTPS,
        http_session=server.client(),
    )
    query = TrinoQuery(request, query="INSERT INTO some_table VALUES (1), (2), (3)")

    result = query.execute()

    assert query.finished is True
    assert query.update_type == "INSERT"
    assert query.update_count == 3
    assert query.stats["state"] == "FINISHED"
    # The count row survives draining and the rows stay lazily iterable.
    assert list(result) == [[3]]
