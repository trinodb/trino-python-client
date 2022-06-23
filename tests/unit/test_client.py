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
import uuid
from unittest import mock
from urllib.parse import urlparse

import httpretty
import pytest
import requests
from httpretty import httprettified
from requests_kerberos.exceptions import KerberosExchangeError

import trino.exceptions
from tests.unit.oauth_test_utils import RedirectHandler, GetTokenCallback, PostStatementCallback, \
    MultithreadedTokenServer, _post_statement_requests, _get_token_requests, REDIRECT_RESOURCE, TOKEN_RESOURCE, \
    SERVER_ADDRESS
from trino import constants
from trino.auth import KerberosAuthentication, _OAuth2TokenBearer
from trino.client import TrinoQuery, TrinoRequest, TrinoResult


@mock.patch("trino.client.TrinoRequest.http")
def test_trino_initial_request(mock_requests, sample_post_response_data):
    mock_requests.Response.return_value.json.return_value = sample_post_response_data

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        user="test",
        source="test",
        catalog="test",
        schema="test",
        http_scheme="http",
        session_properties={},
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
    source = "test_source"
    accept_encoding_header = "accept-encoding"
    accept_encoding_value = "identity,deflate,gzip"
    client_info_header = constants.HEADER_CLIENT_INFO
    client_info_value = "some_client_info"

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        user=user,
        source=source,
        catalog=catalog,
        schema=schema,
        http_scheme="http",
        session_properties={},
        http_headers={
            accept_encoding_header: accept_encoding_value,
            client_info_header: client_info_value,
        },
        redirect_handler=None,
    )

    def assert_headers(headers):
        assert headers[constants.HEADER_CATALOG] == catalog
        assert headers[constants.HEADER_SCHEMA] == schema
        assert headers[constants.HEADER_SOURCE] == source
        assert headers[constants.HEADER_USER] == user
        assert headers[constants.HEADER_SESSION] == ""
        assert headers[accept_encoding_header] == accept_encoding_value
        assert headers[client_info_header] == client_info_value
        assert len(headers.keys()) == 8

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
        user="test_user",
        session_properties={
            "a": "1",
            "b": "2",
            "c": "more=v1,v2"
        }
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
        user="test",
        source="test",
        catalog="test",
        schema="test",
        http_scheme="http",
        session_properties={},
    )

    sql = 'select 1'
    additional_headers = {
        'X-Trino-Fake-1': 'one',
        'X-Trino-Fake-2': 'two',
    }

    combined_headers = req.http_headers
    combined_headers.update(additional_headers)

    req.post(sql, additional_headers)

    # Validate that the post call was performed including the addtional headers
    _, post_kwargs = post.call_args
    assert post_kwargs['headers'] == combined_headers


def test_request_invalid_http_headers():
    with pytest.raises(ValueError) as value_error:
        TrinoRequest(
            host="coordinator",
            port=8080,
            user="test",
            http_headers={constants.HEADER_USER: "invalid_header"},
        )
    assert str(value_error.value).startswith("cannot override reserved HTTP header")


def test_request_client_tags_headers(mock_get_and_post):
    get, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        user="test_user",
        client_tags=["tag1", "tag2"]
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
        user="test_user"
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
        user="test",
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
        user="test",
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
        user="test",
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
            user="test",
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
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token, attempts)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        user="test",
        http_scheme=constants.HTTPS,
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler))
    response = request.post("select 1")

    assert response.request.headers['Authorization'] == f"Bearer {token}"
    assert redirect_handler.redirect_server == redirect_server
    assert get_token_callback.attempts == 0
    assert len(_post_statement_requests()) == 2
    assert len(_get_token_requests(challenge_id)) == attempts


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
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token, attempts)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=f"{TOKEN_RESOURCE}/{challenge_id}",
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        user="test",
        http_scheme=constants.HTTPS,
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler))
    with pytest.raises(trino.exceptions.TrinoAuthError) as exp:
        request.post("select 1")

    assert str(exp.value) == "Exceeded max attempts while getting the token"
    assert redirect_handler.redirect_server == redirect_server
    assert get_token_callback.attempts == attempts - _OAuth2TokenBearer.MAX_OAUTH_ATTEMPTS
    assert len(_post_statement_requests()) == 1
    assert len(_get_token_requests(challenge_id)) == _OAuth2TokenBearer.MAX_OAUTH_ATTEMPTS


@pytest.mark.parametrize("header,error", [
    ("", "Error: header WWW-Authenticate not available in the response."),
    ('Bearer"', 'Error: header info didn\'t have x_redirect_server'),
    ('x_redirect_server="redirect_server", x_token_server="token_server"', 'Error: header info didn\'t match x_redirect_server="redirect_server", x_token_server="token_server"'),  # noqa: E501
    ('Bearer x_redirect_server="redirect_server"', 'Error: header info didn\'t have x_token_server'),
    ('Bearer x_token_server="token_server"', 'Error: header info didn\'t have x_redirect_server'),
])
@httprettified
def test_oauth2_authentication_missing_headers(header, error):
    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        adding_headers={'WWW-Authenticate': header},
        status=401)

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        user="test",
        http_scheme=constants.HTTPS,
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
])
@httprettified
def test_oauth2_header_parsing(header, sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    # noinspection PyUnusedLocal
    def post_statement(request, uri, response_headers):
        authorization = request.headers.get("Authorization")
        if authorization and authorization.replace("Bearer ", "") in token:
            return [200, response_headers, json.dumps(sample_post_response_data)]
        return [401, {'Www-Authenticate': header.format(redirect_server=redirect_server, token_server=token_server),
                      'Basic realm': '"Trino"'}, ""]

    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        body=post_statement)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    response = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        user="test",
        http_scheme=constants.HTTPS,
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler)
    ).post("select 1")

    assert response.request.headers['Authorization'] == f"Bearer {token}"
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
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

    httpretty.register_uri(
        method=httpretty.GET,
        uri=f"{TOKEN_RESOURCE}/{challenge_id}",
        status=http_status,
        body="error")

    redirect_handler = RedirectHandler()

    request = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        user="test",
        http_scheme=constants.HTTPS,
        auth=trino.auth.OAuth2Authentication(redirect_auth_url_handler=redirect_handler))

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
                user="test",
                http_scheme=constants.HTTPS,
                auth=auth)
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
        user="test",
        source="test",
        catalog="test",
        schema="test",
        http_scheme="http",
        session_properties={},
    )

    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = 200
    status = req.process(http_resp)

    assert status.next_uri == sample_get_response_data["nextUri"]
    assert status.id == sample_get_response_data["id"]
    assert status.rows == sample_get_response_data["data"]


@mock.patch("trino.client.TrinoRequest.http")
def test_trino_fetch_error(mock_requests, sample_get_error_response_data):
    mock_requests.Response.return_value.json.return_value = sample_get_error_response_data

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        user="test",
        source="test",
        catalog="test",
        schema="test",
        http_scheme="http",
        session_properties={},
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
def test_trino_connection_error(monkeypatch, error_code, error_type, error_message):
    monkeypatch.setattr(TrinoRequest.http.Response, "json", lambda x: {})

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        user="test",
        source="test",
        catalog="test",
        schema="test",
        http_scheme="http",
        session_properties={},
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
        user="test",
        extra_credential=[("a.username", "foo"), ("b.password", "bar")],
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
            user="test",
            extra_credential=[("a=b", "")],
        )

    assert str(e_info.value) == "whitespace or '=' are disallowed in extra credential 'a=b'"


def test_extra_credential_key_non_ascii():
    with pytest.raises(ValueError) as e_info:
        TrinoRequest(
            host="coordinator",
            port=constants.DEFAULT_TLS_PORT,
            user="test",
            extra_credential=[("的", "")],
        )

    assert str(e_info.value) == "only ASCII characters are allowed in extra credential '的'"


def test_extra_credential_value_encoding(mock_get_and_post):
    _, post = mock_get_and_post

    req = TrinoRequest(
        host="coordinator",
        port=constants.DEFAULT_TLS_PORT,
        user="test",
        extra_credential=[("foo", "bar 的")],
    )

    req.post("SELECT 1")
    _, post_kwargs = post.call_args
    headers = post_kwargs["headers"]
    assert constants.HEADER_EXTRA_CREDENTIAL in headers
    assert headers[constants.HEADER_EXTRA_CREDENTIAL] == "foo=bar+%E7%9A%84"


class RetryRecorder(object):
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


def test_authentication_fail_retry(monkeypatch):
    post_retry = RetryRecorder(error=KerberosExchangeError())
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_retry)

    get_retry = RetryRecorder(error=KerberosExchangeError())
    monkeypatch.setattr(TrinoRequest.http.Session, "get", get_retry)

    attempts = 3
    kerberos_auth = KerberosAuthentication()
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        user="test",
        http_scheme=constants.HTTPS,
        auth=kerberos_auth,
        max_attempts=attempts,
    )

    with pytest.raises(KerberosExchangeError):
        req.post("URL")
    assert post_retry.retry_count == attempts

    with pytest.raises(KerberosExchangeError):
        req.get("URL")
    assert post_retry.retry_count == attempts


@pytest.mark.parametrize("status_code, attempts", [
    (502, 3),
    (503, 3),
    (504, 3),
])
def test_5XX_error_retry(status_code, attempts, monkeypatch):
    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = status_code

    post_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_retry)

    get_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "get", get_retry)

    req = TrinoRequest(
        host="coordinator", port=8080, user="test", max_attempts=attempts
    )

    req.post("URL")
    assert post_retry.retry_count == attempts

    req.get("URL")
    assert post_retry.retry_count == attempts


@pytest.mark.parametrize("status_code", [
    501
])
def test_error_no_retry(status_code, monkeypatch):
    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = status_code

    post_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_retry)

    get_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "get", get_retry)

    req = TrinoRequest(
        host="coordinator", port=8080, user="test", max_attempts=3
    )

    req.post("URL")
    assert post_retry.retry_count == 1

    req.get("URL")
    assert post_retry.retry_count == 1


class FakeGatewayResponse(object):
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


def test_trino_result_response_headers():
    """
    Validates that the `TrinoResult.response_headers` property returns the
    headers associated to the TrinoQuery instance provided to the `TrinoResult`
    class.
    """
    mock_trino_query = mock.Mock(respone_headers={
        'X-Trino-Fake-1': 'one',
        'X-Trino-Fake-2': 'two',
    })

    result = TrinoResult(
        query=mock_trino_query,
    )
    assert result.response_headers == mock_trino_query.response_headers


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
                'X-Trino-Fake-1': 'one',
                'X-Trino-Fake-2': 'two',
            }

        def json(self):
            return sample_get_response_data

    req = TrinoRequest(
        host="coordinator",
        port=8080,
        user="test",
        source="test",
        catalog="test",
        schema="test",
        http_scheme="http",
        session_properties={},
    )

    sql = 'execute my_stament using 1, 2, 3'
    additional_headers = {
        constants.HEADER_PREPARED_STATEMENT: 'my_statement=added_prepare_statement_header'
    }

    # Patch the post function to avoid making the requests, as well as to
    # validate that the function was called with the right arguments.
    with mock.patch.object(req, 'post', return_value=MockResponse()) as mock_post:
        query = TrinoQuery(
            request=req,
            sql=sql
        )
        result = query.execute(additional_http_headers=additional_headers)

        # Validate the the post function was called with the right argguments
        mock_post.assert_called_once_with(sql, additional_headers)

        # Validate the result is an instance of TrinoResult
        assert isinstance(result, TrinoResult)
