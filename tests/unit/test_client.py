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
import json
import threading
import time
import urllib
import uuid
from contextlib import nullcontext as does_not_raise
from typing import Any
from typing import Dict
from typing import Optional
from unittest import mock
from unittest import TestCase
from urllib.parse import urlparse
from zoneinfo import ZoneInfoNotFoundError

import gssapi
import httpretty
import keyring
import pytest
import requests
from httpretty import httprettified
from requests_gssapi.exceptions import SPNEGOExchangeError
from requests_kerberos.exceptions import KerberosExchangeError
from tzlocal import get_localzone_name  # type: ignore

import trino.exceptions
from tests.unit.oauth_test_utils import _get_token_requests
from tests.unit.oauth_test_utils import _post_statement_requests
from tests.unit.oauth_test_utils import GetTokenCallback
from tests.unit.oauth_test_utils import MultithreadedTokenServer
from tests.unit.oauth_test_utils import PostStatementCallback
from tests.unit.oauth_test_utils import REDIRECT_RESOURCE
from tests.unit.oauth_test_utils import RedirectHandler
from tests.unit.oauth_test_utils import RedirectHandlerWithException
from tests.unit.oauth_test_utils import SERVER_ADDRESS
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
from trino.client import TrinoQuery
from trino.client import TrinoRequest
from trino.client import TrinoResult


@mock.patch("trino.client.TrinoRequest.http")
def test_trino_initial_request(mock_requests, sample_post_response_data):
    mock_requests.Response.return_value.json.return_value = sample_post_response_data

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

    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = 200
    status = req.process(http_resp)

    assert status.next_uri == sample_post_response_data["nextUri"]
    assert status.id == sample_post_response_data["id"]


def test_request_headers(mock_get_and_post):
    get, post = mock_get_and_post

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
                },
            ),
            http_scheme="http",
        )

    def assert_headers(headers):
        assert headers[constants.HEADER_CATALOG] == catalog
        assert headers[constants.HEADER_SCHEMA] == schema
        assert headers[constants.HEADER_SOURCE] == source
        assert headers[constants.HEADER_USER] == user
        assert headers[constants.HEADER_AUTHORIZATION_USER] == authorization_user
        assert headers[constants.HEADER_SESSION] == ""
        assert headers[constants.HEADER_TRANSACTION] is None
        assert headers[constants.HEADER_TIMEZONE] == timezone
        assert headers[constants.HEADER_CLIENT_CAPABILITIES] == "PARAMETRIC_DATETIME"
        assert headers[accept_encoding_header] == accept_encoding_value
        assert headers[client_info_header] == client_info_value
        assert headers[constants.HEADER_ROLE] == (
            "hive=ALL,"
            "system=" + urllib.parse.quote("ROLE{analyst}") + ","
            "catalog1=NONE,"
            "catalog2=" + urllib.parse.quote("ROLE{catalog2_role}")
        )
        assert headers["User-Agent"] == f"{constants.CLIENT_NAME}/{__version__}"
        assert len(headers.keys()) == 13

    req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers(post_kwargs["headers"])

    req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers(get_kwargs["headers"])


def test_request_session_properties_headers(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test_user", properties={"a": "1", "b": "2", "c": "more=v1,v2"}),
    )

    def assert_headers(headers):
        assert headers[constants.HEADER_SESSION] == "a=1,b=2,c=more%3Dv1%2Cv2"

    req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers(post_kwargs["headers"])

    req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers(get_kwargs["headers"])


def test_additional_request_post_headers(mock_get_and_post):
    """
    Tests that the `TrinoRequest.post` function can take addtional headers
    and that it combines them with the existing ones to perform the request.
    """
    _, post = mock_get_and_post

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

    sql = "select 1"
    additional_headers = {
        "X-Trino-Fake-1": "one",
        "X-Trino-Fake-2": "two",
    }

    combined_headers = req.http_headers
    combined_headers.update(additional_headers)

    req.post(sql, additional_headers)

    # Validate that the post call was performed including the addtional headers
    _, post_kwargs = post.call_args
    assert post_kwargs["headers"] == combined_headers


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


def test_request_client_tags_headers(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test_user", client_tags=["tag1", "tag2"]),
    )

    def assert_headers(headers):
        assert headers[constants.HEADER_CLIENT_TAGS] == "tag1,tag2"

    req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers(post_kwargs["headers"])

    req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers(get_kwargs["headers"])


def test_request_client_tags_headers_no_client_tags(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
        ),
    )

    def assert_headers(headers):
        assert constants.HEADER_CLIENT_TAGS not in headers

    req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers(post_kwargs["headers"])

    req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers(get_kwargs["headers"])


def test_enabling_https_automatically_when_using_port_443(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
    )

    req.post("SELECT 1")
    post_args, _ = post.call_args
    parsed_url = urlparse(post_args[0])

    assert parsed_url.scheme == constants.HTTPS


def test_https_scheme(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
    )

    req.post("SELECT 1")
    post_args, _ = post.call_args
    parsed_url = urlparse(post_args[0])

    assert parsed_url.scheme == constants.HTTPS
    assert parsed_url.port == constants.DEFAULT_TLS_PORT


def test_http_scheme_with_port(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTP,
    )

    req.post("SELECT 1")
    post_args, _ = post.call_args
    parsed_url = urlparse(post_args[0])

    assert parsed_url.scheme == constants.HTTP
    assert parsed_url.port == constants.DEFAULT_TLS_PORT


def test_request_timeout():
    timeout = 0.1
    http_scheme = "http"
    host = "coordinator"
    port = 8080
    url = http_scheme + "://" + host + ":" + str(port) + constants.URL_STATEMENT_PATH

    def long_call(request, uri, headers):
        time.sleep(timeout * 2)
        return (200, headers, "delayed success")

    httpretty.enable()
    for method in [httpretty.POST, httpretty.GET]:
        httpretty.register_uri(method, url, body=long_call)

    # timeout without retry
    for request_timeout in [timeout, (timeout, timeout)]:
        req = TrinoRequest(
            host=host,
            port=port,
            client_session=ClientSession(
                user="test",
            ),
            http_scheme=http_scheme,
            max_attempts=1,
            request_timeout=request_timeout,
        )

        with pytest.raises(requests.exceptions.Timeout):
            req.get(url)

        with pytest.raises(requests.exceptions.Timeout):
            req.post("select 1")

    httpretty.disable()
    httpretty.reset()


@pytest.mark.parametrize("attempts", [1, 3, 5])
@httprettified
def test_oauth2_authentication_flow(attempts, sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)

    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST, uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}", body=post_statement_callback
    )

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token, attempts)
    httpretty.register_uri(method=httpretty.GET, uri=token_server, body=get_token_callback)

    redirect_handler = RedirectHandler()

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
    )
    response = request.post("select 1")

    assert response.request.headers["Authorization"] == f"Bearer {token}"
    assert redirect_handler.redirect_server == redirect_server
    assert get_token_callback.attempts == 0
    assert len(_post_statement_requests()) == 2
    assert len(_get_token_requests(challenge_id)) == attempts


@httprettified
def test_oauth2_refresh_token_flow(sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(None, token_server, [token], sample_post_response_data)

    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST, uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}", body=post_statement_callback
    )

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(method=httpretty.GET, uri=token_server, body=get_token_callback)

    redirect_handler = RedirectHandlerWithException(
        trino.exceptions.TrinoAuthError("Do not use redirect handler when there is no redirect_uri in the response")
    )

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
    )

    response = request.post("select 1")

    assert response.request.headers["Authorization"] == f"Bearer {token}"
    assert get_token_callback.attempts == 0
    assert len(_post_statement_requests()) == 2


@pytest.mark.parametrize("attempts", [6, 10])
@httprettified
def test_oauth2_exceed_max_attempts(attempts, sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)

    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST, uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}", body=post_statement_callback
    )

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token, attempts)
    httpretty.register_uri(method=httpretty.GET, uri=f"{TOKEN_RESOURCE}/{challenge_id}", body=get_token_callback)

    redirect_handler = RedirectHandler()

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
    )
    with pytest.raises(trino.exceptions.TrinoAuthError) as exp:
        request.post("select 1")

    assert str(exp.value) == "Exceeded max attempts while getting the token"
    assert redirect_handler.redirect_server == redirect_server
    assert get_token_callback.attempts == attempts - _OAuth2TokenBearer.MAX_OAUTH_ATTEMPTS
    assert len(_post_statement_requests()) == 1
    assert len(_get_token_requests(challenge_id)) == _OAuth2TokenBearer.MAX_OAUTH_ATTEMPTS


@pytest.mark.parametrize(
    "header,error",
    [
        ("", "Error: header WWW-Authenticate not available in the response."),
        ('Bearer"', "Error: header info didn't have x_token_server"),
        (
            'x_redirect_server="redirect_server", x_token_server="token_server"',
            'Error: header info didn\'t match x_redirect_server="redirect_server", x_token_server="token_server"',
        ),  # noqa: E501
        ('Bearer x_redirect_server="redirect_server"', "Error: header info didn't have x_token_server"),
    ],
)
@httprettified
def test_oauth2_authentication_missing_headers(header, error):
    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        adding_headers={"WWW-Authenticate": header},
        status=401,
    )

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=RedirectHandler()),
    )

    with pytest.raises(trino.exceptions.TrinoAuthError) as exp:
        request.post("select 1")

    assert str(exp.value) == error


@pytest.mark.parametrize(
    "header",
    [
        'Bearer x_redirect_server="{redirect_server}", x_token_server="{token_server}", additional_challenge',
        'Bearer x_redirect_server="{redirect_server}", x_token_server="{token_server}", additional_challenge="value"',
        'Bearer x_token_server="{token_server}", x_redirect_server="{redirect_server}"',
        'Basic realm="Trino", Bearer x_redirect_server="{redirect_server}", x_token_server="{token_server}"',
        'Bearer x_redirect_server="{redirect_server}", x_token_server="{token_server}", Basic realm="Trino"',
        'Basic realm="Trino", Bearer realm="Trino", token_type="JWT", Bearer x_redirect_server="{redirect_server}", '
        'x_token_server="{token_server}"'
        'Bearer x_redirect_server="{redirect_server}",x_token_server="{token_server}",additional_challenge',
    ],
)
@httprettified
def test_oauth2_header_parsing(header, sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}?role=test"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    # noinspection PyUnusedLocal
    def post_statement(request, uri, response_headers):
        authorization = request.headers.get("Authorization")
        if authorization and authorization.replace("Bearer ", "") in token:
            return [200, response_headers, json.dumps(sample_post_response_data)]
        return [
            401,
            {
                "Www-Authenticate": header.format(redirect_server=redirect_server, token_server=token_server),
                "Basic realm": '"Trino"',
            },
            "",
        ]

    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST, uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}", body=post_statement
    )

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(method=httpretty.GET, uri=token_server, body=get_token_callback)

    redirect_handler = RedirectHandler()

    response = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
    ).post("select 1")

    assert response.request.headers["Authorization"] == f"Bearer {token}"
    assert redirect_handler.redirect_server == redirect_server
    assert get_token_callback.attempts == 0
    assert len(_post_statement_requests()) == 2
    assert len(_get_token_requests(challenge_id)) == 1


@pytest.mark.parametrize("http_status", [400, 401, 500])
@httprettified
def test_oauth2_authentication_fail_token_server(http_status, sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)

    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST, uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}", body=post_statement_callback
    )

    httpretty.register_uri(
        method=httpretty.GET, uri=f"{TOKEN_RESOURCE}/{challenge_id}", status=http_status, body="error"
    )

    redirect_handler = RedirectHandler()

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
    )

    with pytest.raises(trino.exceptions.TrinoAuthError) as exp:
        request.post("select 1")

    assert redirect_handler.redirect_server == redirect_server
    assert str(exp.value) == f"Error while getting the token response status code: {http_status}, body: error"
    assert len(_post_statement_requests()) == 1
    assert len(_get_token_requests(challenge_id)) == 1


@httprettified
def test_multithreaded_oauth2_authentication_flow(sample_post_response_data):
    redirect_handler = RedirectHandler()
    auth = trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler)

    token_server = MultithreadedTokenServer(sample_post_response_data)

    class RunningThread(threading.Thread):
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
                auth=auth,
            )
            for i in range(10):
                # apparently HTTPretty in the current version is not thread-safe
                # https://github.com/gabrielfalcao/HTTPretty/issues/209
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
        assert len(_get_token_requests(challenge_id)) == 1
    # 3 threads * (10 POST /statement each + 1 replied request by authentication)
    assert len(_post_statement_requests()) == 31


@mock.patch("trino.client.TrinoRequest.http")
def test_trino_fetch_request(mock_requests, sample_get_response_data):
    mock_requests.Response.return_value.json.return_value = sample_get_response_data

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

    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = 200
    status = req.process(http_resp)

    assert status.next_uri == sample_get_response_data["nextUri"]
    assert status.id == sample_get_response_data["id"]
    assert status.rows == sample_get_response_data["data"]


@mock.patch("trino.client.TrinoRequest.http")
def test_trino_fetch_request_data_none(mock_requests, sample_get_response_data_none):
    mock_requests.Response.return_value.json.return_value = sample_get_response_data_none

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

    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = 200
    status = req.process(http_resp)

    assert status.next_uri == sample_get_response_data_none["nextUri"]
    assert status.id == sample_get_response_data_none["id"]
    assert status.rows == []


@mock.patch("trino.client.TrinoRequest.http")
def test_trino_fetch_error(mock_requests, sample_get_error_response_data):
    mock_requests.Response.return_value.json.return_value = sample_get_error_response_data

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

    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = 200
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
    assert error.message == "line 1:15: Schema must be specified when session schema is not set"
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
def test_trino_connection_error(monkeypatch, error_code, error_type, error_message):
    monkeypatch.setattr(TrinoRequest.http.Response, "json", lambda x: {})

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

    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = error_code
    with pytest.raises(error_type) as error:
        req.process(http_resp)
    assert error_message in str(error)


def test_extra_credential(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
            extra_credential=[("a.username", "foo"), ("b.password", "bar")],
        ),
    )

    req.post("SELECT 1")
    _, post_kwargs = post.call_args
    headers = post_kwargs["headers"]
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


def test_extra_credential_value_encoding(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(
            user="test",
            extra_credential=[("foo", "bar 的")],
        ),
    )

    req.post("SELECT 1")
    _, post_kwargs = post.call_args
    headers = post_kwargs["headers"]
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "foo=bar+%E7%9A%84"


def test_extra_credential_value_object(mock_get_and_post):
    _, post = mock_get_and_post

    class TestCredential:
        value = "initial"

        def __str__(self):
            return self.value

    credential = TestCredential()

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        client_session=ClientSession(user="test", extra_credential=[("foo", credential)]),
    )

    req.post("SELECT 1")
    _, post_kwargs = post.call_args
    headers = post_kwargs["headers"]
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "foo=initial"

    # Make a second request, assert that credential has changed
    credential.value = "changed"
    req.post("SELECT 1")
    _, post_kwargs = post.call_args
    headers = post_kwargs["headers"]
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "foo=changed"


class MockGssapiCredentials:
    def __init__(self, name: gssapi.Name, usage: str):
        self.name = name
        self.usage = usage

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MockGssapiCredentials):
            return False
        return (
            self.name == other.name,
            self.usage == other.usage,
        )


@pytest.fixture
def mock_gssapi_creds(monkeypatch):
    monkeypatch.setattr("gssapi.Credentials", MockGssapiCredentials)


def _gssapi_uname(spn: str):
    return gssapi.Name(spn, gssapi.NameType.user)


def _gssapi_sname(principal: str):
    return gssapi.Name(principal, gssapi.NameType.hostbased_service)


@pytest.mark.parametrize(
    "options, expected_credentials, expected_hostname, expected_exception",
    [
        (
            {},
            None,
            None,
            does_not_raise(),
        ),
        (
            {"hostname_override": "foo"},
            None,
            "foo",
            does_not_raise(),
        ),
        (
            {"service_name": "bar"},
            None,
            None,
            pytest.raises(ValueError, match=r"must be used together with hostname_override"),
        ),
        (
            {"hostname_override": "foo", "service_name": "bar"},
            None,
            _gssapi_sname("bar@foo"),
            does_not_raise(),
        ),
        (
            {"principal": "foo"},
            MockGssapiCredentials(_gssapi_uname("foo"), "initial"),
            None,
            does_not_raise(),
        ),
    ],
)
def test_authentication_gssapi_init_arguments(
    options,
    expected_credentials,
    expected_hostname,
    expected_exception,
    mock_gssapi_creds,
    monkeypatch,
):
    auth = GSSAPIAuthentication(**options)

    session = requests.Session()

    with expected_exception:
        auth.set_http_session(session)

        assert session.auth.target_name == expected_hostname
        assert session.auth.creds == expected_credentials


class RetryRecorder:
    def __init__(self, error=None, result=None):
        self.__name__ = "RetryRecorder"
        self._retry_count = 0
        self._error = error
        self._result = result

    def __call__(self, *args, **kwargs):
        self._retry_count += 1
        if self._error is not None:
            raise self._error
        if self._result is not None:
            return self._result

    @property
    def retry_count(self):
        return self._retry_count


@pytest.mark.parametrize(
    "auth_class, retry_exception_class",
    [
        (KerberosAuthentication, KerberosExchangeError),
        (GSSAPIAuthentication, SPNEGOExchangeError),
    ],
)
def test_authentication_fail_retry(auth_class, retry_exception_class, monkeypatch):
    post_retry = RetryRecorder(error=retry_exception_class())
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_retry)

    get_retry = RetryRecorder(error=retry_exception_class())
    monkeypatch.setattr(TrinoRequest.http.Session, "get", get_retry)

    attempts = 3
    kerberos_auth = auth_class()
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        http_scheme=constants.HTTPS,
        auth=kerberos_auth,
        max_attempts=attempts,
    )

    with pytest.raises(retry_exception_class):
        req.post("URL")
    assert post_retry.retry_count == attempts

    with pytest.raises(retry_exception_class):
        req.get("URL")
    assert post_retry.retry_count == attempts


@pytest.mark.parametrize(
    "status_code, attempts",
    [
        (502, 3),
        (503, 3),
        (504, 3),
    ],
)
def test_5XX_error_retry(status_code, attempts, monkeypatch):
    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = status_code

    post_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_retry)

    get_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "get", get_retry)

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        max_attempts=attempts,
    )

    req.post("URL")
    assert post_retry.retry_count == attempts

    req.get("URL")
    assert post_retry.retry_count == attempts


def test_429_error_retry(monkeypatch):
    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = 429
    http_resp.headers["Retry-After"] = 1

    post_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_retry)

    get_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "get", get_retry)

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        max_attempts=3,
    )

    req.post("URL")
    assert post_retry.retry_count == 3

    req.get("URL")
    assert post_retry.retry_count == 3


@pytest.mark.parametrize("status_code", [501])
def test_error_no_retry(status_code, monkeypatch):
    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = status_code

    post_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_retry)

    get_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "get", get_retry)

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test",
        ),
        max_attempts=3,
    )

    req.post("URL")
    assert post_retry.retry_count == 1

    req.get("URL")
    assert post_retry.retry_count == 1


class FakeGatewayResponse:
    def __init__(self, http_response, redirect_count=1):
        self.__name__ = "FakeGatewayResponse"
        self.http_response = http_response
        self.redirect_count = redirect_count
        self.count = 0

    def __call__(self, *args, **kwargs):
        self.count += 1
        if self.count == self.redirect_count:
            return self.http_response
        http_response = TrinoRequest.http.Response()
        http_response.status_code = 301
        http_response.headers["Location"] = "http://1.2.3.4:8080/new-path/"
        assert http_response.is_redirect
        return http_response


def test_trino_query_response_headers(sample_get_response_data):
    """
    Validates that the `TrinoQuery.execute` function can take addtional headers
    that are pass the the provided request instance post function call and it
    returns a `TrinoResult` instance.
    """

    class MockResponse(mock.Mock):
        # Fake response class
        @property
        def headers(self):
            return {
                "X-Trino-Fake-1": "one",
                "X-Trino-Fake-2": "two",
            }

        def json(self):
            return sample_get_response_data

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

    sql = "execute my_stament using 1, 2, 3"
    additional_headers = {
        constants.HEADER_PREPARED_STATEMENT: "my_statement=added_prepare_statement_header",
        constants.HEADER_CLIENT_CAPABILITIES: "PARAMETRIC_DATETIME",
    }

    # Patch the post function to avoid making the requests, as well as to
    # validate that the function was called with the right arguments.
    with mock.patch.object(req, "post", return_value=MockResponse()) as mock_post:
        query = TrinoQuery(request=req, query=sql)
        result = query.execute(additional_http_headers=additional_headers)

        # Validate the the post function was called with the right argguments
        mock_post.assert_called_once_with(sql, additional_headers)

        # Validate the result is an instance of TrinoResult
        assert isinstance(result, TrinoResult)


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


def test_request_headers_role_hive_all(mock_get_and_post):
    get, post = mock_get_and_post
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test_user", roles={"hive": "ALL"}),
    )

    req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers_with_roles(post_kwargs["headers"], "hive=ALL")

    req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers_with_roles(post_kwargs["headers"], "hive=ALL")


def test_request_headers_role_admin(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test_user", roles={"system": "admin"}),
    )
    roles = "system=" + urllib.parse.quote("ROLE{admin}")

    req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers_with_roles(post_kwargs["headers"], roles)

    req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers_with_roles(post_kwargs["headers"], roles)


def test_request_headers_role_empty(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
            roles=None,
        ),
    )

    req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers_with_roles(post_kwargs["headers"], None)

    req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers_with_roles(post_kwargs["headers"], None)


def assert_headers_timezone(headers: Dict[str, str], timezone: str):
    assert headers[constants.HEADER_TIMEZONE] == timezone


def test_request_headers_with_timezone(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test_user", timezone="Europe/Brussels"),
    )

    req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers_timezone(post_kwargs["headers"], "Europe/Brussels")

    req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers_timezone(post_kwargs["headers"], "Europe/Brussels")


def test_request_headers_without_timezone(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(
            user="test_user",
        ),
    )
    localzone = get_localzone_name()

    req.post("URL")
    _, post_kwargs = post.call_args
    assert_headers_timezone(post_kwargs["headers"], localzone)

    req.get("URL")
    _, get_kwargs = get.call_args
    assert_headers_timezone(post_kwargs["headers"], localzone)


def test_request_with_invalid_timezone(mock_get_and_post):
    with pytest.raises(ZoneInfoNotFoundError) as zinfo_error:
        TrinoRequest(
            host="coordinator",
            port=8080,
            client_session=ClientSession(user="test_user", timezone="INVALID_TIMEZONE"),
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
