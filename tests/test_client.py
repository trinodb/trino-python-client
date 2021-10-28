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

import httpretty
import pytest
import requests
import time
from unittest import mock
from urllib.parse import urlparse


from requests_kerberos.exceptions import KerberosExchangeError
from trino.client import TrinoQuery, TrinoRequest, TrinoResult
from trino.auth import KerberosAuthentication
from trino import constants
import trino.exceptions


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


def test_503_error_retry(monkeypatch):
    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = 503

    post_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_retry)

    get_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "get", get_retry)

    attempts = 3
    req = TrinoRequest(
        host="coordinator", port=8080, user="test", max_attempts=attempts
    )

    req.post("URL")
    assert post_retry.retry_count == attempts

    req.get("URL")
    assert post_retry.retry_count == attempts


def test_504_error_retry(monkeypatch):
    http_resp = TrinoRequest.http.Response()
    http_resp.status_code = 504

    post_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_retry)

    get_retry = RetryRecorder(result=http_resp)
    monkeypatch.setattr(TrinoRequest.http.Session, "get", get_retry)

    attempts = 3
    req = TrinoRequest(
        host="coordinator", port=8080, user="test", max_attempts=attempts
    )

    req.post("URL")
    assert post_retry.retry_count == attempts

    req.get("URL")
    assert post_retry.retry_count == attempts


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
