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
import uuid
from unittest.mock import patch

import httpretty
import pytest
from httpretty import httprettified
from requests import Session

import trino.exceptions
from tests.unit.oauth_test_utils import _get_token_requests
from tests.unit.oauth_test_utils import _post_statement_requests
from tests.unit.oauth_test_utils import GetTokenCallback
from tests.unit.oauth_test_utils import PostStatementCallback
from tests.unit.oauth_test_utils import REDIRECT_RESOURCE
from tests.unit.oauth_test_utils import RedirectHandler
from tests.unit.oauth_test_utils import SERVER_ADDRESS
from tests.unit.oauth_test_utils import TOKEN_RESOURCE
from trino import constants
from trino.auth import BasicAuthentication
from trino.auth import OAuth2Authentication
from trino.dbapi import connect
from trino.dbapi import Connection


@patch("trino.dbapi.trino.client")
def test_http_session_is_correctly_passed_in(mock_client):
    # GIVEN
    test_session = Session()
    test_session.proxies = {"http": "some.http.proxy:81", "https": "some.http.proxy:81"}

    # WHEN
    with connect("sample_trino_cluster:443", http_session=test_session) as conn:
        conn.cursor().execute("SOME FAKE QUERY")

    # THEN
    request_args, _ = mock_client.TrinoRequest.call_args
    assert test_session in request_args


@patch("trino.dbapi.trino.client")
def test_http_session_is_defaulted_when_not_specified(mock_client):
    # WHEN
    with connect("sample_trino_cluster:443") as conn:
        conn.cursor().execute("SOME FAKE QUERY")

    # THEN
    request_args, _ = mock_client.TrinoRequest.call_args
    assert mock_client.TrinoRequest.http.Session.return_value in request_args


@httprettified
def test_token_retrieved_once_per_auth_instance(sample_post_response_data, sample_get_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    get_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_get_response_data)

    # bind post statement to submit query
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

    # bind get statement for result retrieval
    httpretty.register_uri(
        method=httpretty.GET,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}/20210817_140827_00000_arvdv/1",
        body=get_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    with connect(
            "coordinator",
            user="test",
            auth=OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
            http_scheme=constants.HTTPS
    ) as conn:
        conn.cursor().execute("SELECT 1")
        conn.cursor().execute("SELECT 2")
        conn.cursor().execute("SELECT 3")

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    with connect(
            "coordinator",
            user="test",
            auth=OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
            http_scheme=constants.HTTPS
    ) as conn2:
        conn2.cursor().execute("SELECT 1")
        conn2.cursor().execute("SELECT 2")
        conn2.cursor().execute("SELECT 3")

    assert len(_get_token_requests(challenge_id)) == 1


@httprettified
def test_token_retrieved_once_when_authentication_instance_is_shared(sample_post_response_data,
                                                                     sample_get_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    get_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_get_response_data)

    # bind post statement to submit query
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

    # bind get statement for result retrieval
    httpretty.register_uri(
        method=httpretty.GET,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}/20210817_140827_00000_arvdv/1",
        body=get_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    authentication = OAuth2Authentication(redirect_auth_url_handler=redirect_handler)

    with connect(
            "coordinator",
            user="test",
            auth=authentication,
            http_scheme=constants.HTTPS
    ) as conn:
        conn.cursor().execute("SELECT 1")
        conn.cursor().execute("SELECT 2")
        conn.cursor().execute("SELECT 3")

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    with connect(
            "coordinator",
            user="test",
            auth=authentication,
            http_scheme=constants.HTTPS
    ) as conn2:
        conn2.cursor().execute("SELECT 1")
        conn2.cursor().execute("SELECT 2")
        conn2.cursor().execute("SELECT 3")

    assert len(_post_statement_requests()) == 7
    assert len(_get_token_requests(challenge_id)) == 1


@httprettified
def test_token_retrieved_once_when_multithreaded(sample_post_response_data, sample_get_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    get_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_get_response_data)

    # bind post statement to submit query
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

    # bind get statement for result retrieval
    httpretty.register_uri(
        method=httpretty.GET,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}/20210817_140827_00000_arvdv/1",
        body=get_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    authentication = OAuth2Authentication(redirect_auth_url_handler=redirect_handler)

    conn = connect(
        "coordinator",
        user="test",
        auth=authentication,
        http_scheme=constants.HTTPS
    )

    class RunningThread(threading.Thread):
        lock = threading.Lock()

        def __init__(self):
            super().__init__()

        def run(self) -> None:
            with RunningThread.lock:
                conn.cursor().execute("SELECT 1")

    threads = [
        RunningThread(),
        RunningThread(),
        RunningThread()
    ]

    # run and join all threads
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(_get_token_requests(challenge_id)) == 1


@patch("trino.dbapi.trino.client")
def test_tags_are_set_when_specified(mock_client):
    client_tags = ["TAG1", "TAG2"]
    with connect("sample_trino_cluster:443", client_tags=client_tags) as conn:
        conn.cursor().execute("SOME FAKE QUERY")

    _, passed_client_tags = mock_client.ClientSession.call_args
    assert passed_client_tags["client_tags"] == client_tags


@patch("trino.dbapi.trino.client")
def test_role_is_set_when_specified(mock_client):
    roles = {"system": "finance"}
    with connect("sample_trino_cluster:443", roles=roles) as conn:
        conn.cursor().execute("SOME FAKE QUERY")

    _, passed_role = mock_client.ClientSession.call_args
    assert passed_role["roles"] == roles


@pytest.mark.parametrize(
    "host, expected_port, expected_scheme",
    [
        ("https://mytrinoserver.domain:9999", 9999, constants.HTTPS),
        ("https://mytrinoserver.domain", constants.DEFAULT_TLS_PORT, constants.HTTPS),
        ("http://mytrinoserver.domain:9999", 9999, constants.HTTP),
        ("http://mytrinoserver.domain", constants.DEFAULT_PORT, constants.HTTP),
        ("mytrinoserver.domain", constants.DEFAULT_PORT, constants.HTTP),
        ("mytrinoserver.domain:9999", 9999, constants.HTTP),
        ("mytrinoserver.domain:443", constants.DEFAULT_TLS_PORT, constants.HTTPS),
    ],
)
def test_hostname_parsing(host, expected_port, expected_scheme):
    connection = Connection(host)
    assert connection.host == "mytrinoserver.domain"
    assert connection.port == expected_port
    assert connection.http_scheme == expected_scheme


def test_hostname_is_lowercased():
    assert Connection("MyTrinoServer.Domain").host == "mytrinoserver.domain"
    assert Connection("https://MyTrinoServer.Domain").host == "mytrinoserver.domain"


@pytest.mark.parametrize(
    "host, expected_message",
    [
        ("http://mytrinoserver.domain/some_path", "a path is not allowed"),
        ("https://mytrinoserver.domain:9999/some_path", "a path is not allowed"),
        ("mytrinoserver.domain/some_path", "a path is not allowed"),
        ("http://mytrinoserver.domain/", "a path is not allowed"),
        ("mytrinoserver.domain/", "a path is not allowed"),
        ("user@mytrinoserver.domain", "credentials are not allowed"),
        ("https://user:password@mytrinoserver.domain", "credentials are not allowed"),
        ("mytrinoserver.domain?key=value", "a query or fragment is not allowed"),
        ("mytrinoserver.domain#fragment", "a query or fragment is not allowed"),
        ("", "the hostname is empty"),
        ("http://", "the hostname is empty"),
        ("https://:8080", "the hostname is empty"),
    ],
)
def test_invalid_host_is_rejected(host, expected_message):
    with pytest.raises(ValueError, match=f"Invalid 'host' argument .*: {expected_message}"):
        Connection(host)


@pytest.mark.parametrize(
    "host, expected_host, expected_port, expected_url",
    [
        ("http://[::1]:8080", "::1", 8080, "http://[::1]:8080/v1/statement"),
        ("[::1]", "::1", constants.DEFAULT_PORT, "http://[::1]:8080/v1/statement"),
        ("[::1]:9999", "::1", 9999, "http://[::1]:9999/v1/statement"),
        ("::1", "::1", constants.DEFAULT_PORT, "http://[::1]:8080/v1/statement"),
        ("https://[::1]", "::1", constants.DEFAULT_TLS_PORT, "https://[::1]:443/v1/statement"),
        ("[2001:db8::1]:9999", "2001:db8::1", 9999, "http://[2001:db8::1]:9999/v1/statement"),
    ],
)
def test_ipv6_hostname_parsing(host, expected_host, expected_port, expected_url):
    connection = Connection(host)
    # Stored unbracketed, bracketed only in the URL.
    assert connection.host == expected_host
    assert connection.port == expected_port
    assert connection._create_request().statement_url == expected_url


def test_description_is_none_when_cursor_is_not_executed():
    connection = Connection("sample_trino_cluster:443")
    with connection.cursor() as cursor:
        assert cursor.description is None


@pytest.mark.parametrize(
    "host, port, http_scheme, expected_http_scheme, expected_port",
    [
        # Decided by a scheme in host, which the http_scheme argument may repeat
        ("https://mytrinoserver.domain", None, None, constants.HTTPS, constants.DEFAULT_TLS_PORT),
        ("http://mytrinoserver.domain", None, None, constants.HTTP, constants.DEFAULT_PORT),
        ("https://mytrinoserver.domain", None, "HTTPS", constants.HTTPS, constants.DEFAULT_TLS_PORT),
        # Decided by the http_scheme argument
        ("mytrinoserver.domain", constants.DEFAULT_TLS_PORT, constants.HTTP, constants.HTTP,
         constants.DEFAULT_TLS_PORT),
        ("mytrinoserver.domain", constants.DEFAULT_PORT, constants.HTTPS, constants.HTTPS, constants.DEFAULT_PORT),
        ("mytrinoserver.domain", None, constants.HTTPS, constants.HTTPS, constants.DEFAULT_TLS_PORT),
        # Decided by the http_scheme argument, which is case-insensitive
        ("mytrinoserver.domain", None, "HTTPS", constants.HTTPS, constants.DEFAULT_TLS_PORT),
        ("mytrinoserver.domain", constants.DEFAULT_TLS_PORT, "Http", constants.HTTP, constants.DEFAULT_TLS_PORT),
        # Decided by the port, whether it arrived in host or in port
        ("mytrinoserver.domain", constants.DEFAULT_TLS_PORT, None, constants.HTTPS, constants.DEFAULT_TLS_PORT),
        ("mytrinoserver.domain:443", None, None, constants.HTTPS, constants.DEFAULT_TLS_PORT),
        ("mytrinoserver.domain", constants.DEFAULT_PORT, None, constants.HTTP, constants.DEFAULT_PORT),
        # Decided by nothing
        ("mytrinoserver.domain", None, None, constants.HTTP, constants.DEFAULT_PORT),
    ],
)
def test_setting_http_scheme(host, port, http_scheme, expected_http_scheme, expected_port):
    """A scheme in host and the http_scheme argument both win over the port.

    A port the caller did not give is inferred back from the resolved scheme.
    """
    connection = Connection(host, port, http_scheme=http_scheme)
    assert connection.http_scheme == expected_http_scheme
    assert connection.port == expected_port


@pytest.mark.parametrize(
    "host, http_scheme, expected_message",
    [
        ("https://mytrinoserver.domain", constants.HTTP, "contradicts http_scheme"),
        ("http://mytrinoserver.domain", constants.HTTPS, "contradicts http_scheme"),
        ("mytrinoserver.domain", "", "Invalid http_scheme"),
        ("mytrinoserver.domain", "ftp", "Invalid http_scheme"),
        ("mytrinoserver.domain", "gopher", "Invalid http_scheme"),
        ("mytrinoserver.domain", "htp", "Invalid http_scheme"),
        ("ftp://mytrinoserver.domain", None, "Invalid scheme 'ftp' in host"),
        ("gopher://mytrinoserver.domain", None, "Invalid scheme 'gopher' in host"),
    ],
)
def test_invalid_scheme_is_rejected(host, http_scheme, expected_message):
    with pytest.raises(ValueError, match=expected_message):
        Connection(host, user="test", http_scheme=http_scheme)


def test_uppercase_http_scheme_still_requires_tls_for_authentication():
    with pytest.raises(trino.exceptions.TrinoAuthError, match="TLS/SSL is required for authentication"):
        Connection("mytrinoserver.domain", user="test", auth=BasicAuthentication("test", "pass"), http_scheme="HTTP")


@pytest.mark.parametrize("host", ["mytrinoserver.domain:x", "mytrinoserver.domain:-1"])
def test_unreadable_port_in_host_is_rejected(host):
    with pytest.raises(ValueError, match="expected a port number"):
        Connection(host, user="test")


@patch("trino.client.CODECS_UNAVAILABLE", {"lz4": "Not installed", "zstd": "Not installed"})
def test_default_encoding_no_compression():
    connection = Connection("host", 8080, user="test")
    assert connection._client_session.encoding == ["json"]


@patch("trino.client.CODECS_UNAVAILABLE", {"zstd": "Not installed"})
def test_default_encoding_lz4():
    connection = Connection("host", 8080, user="test")
    assert connection._client_session.encoding == ["json+lz4", "json"]


@patch("trino.client.CODECS_UNAVAILABLE", {"lz4": "Not installed"})
def test_default_encoding_zstd():
    connection = Connection("host", 8080, user="test")
    assert connection._client_session.encoding == ["json+zstd", "json"]


@patch("trino.client.CODECS_UNAVAILABLE", {})
def test_default_encoding_all():
    connection = Connection("host", 8080, user="test")
    assert connection._client_session.encoding == ["json+zstd", "json+lz4", "json"]


def test_error_when_auth_over_http():
    with pytest.raises(trino.exceptions.TrinoAuthError, match="TLS/SSL is required for authentication"):
        Connection("mytrinoserver.domain", http_scheme=constants.HTTP, auth=BasicAuthentication("u", "p"))


def test_no_error_when_auth_over_https():
    Connection("mytrinoserver.domain", http_scheme=constants.HTTPS, auth=BasicAuthentication("u", "p"))


def test_error_when_auth_over_http_mentions_allow_insecure_auth():
    with pytest.raises(trino.exceptions.TrinoAuthError, match="allow_insecure_auth=True"):
        Connection("mytrinoserver.domain", http_scheme=constants.HTTP, auth=BasicAuthentication("u", "p"))


def test_no_error_when_auth_over_http_with_allow_insecure_auth():
    connection = Connection(
        "mytrinoserver.domain",
        http_scheme=constants.HTTP,
        auth=BasicAuthentication("u", "p"),
        allow_insecure_auth=True,
    )
    assert connection.http_scheme == constants.HTTP
    # Ensure the flag doesn't just suppress the constructor check, but also
    # doesn't prevent building a request object that would actually be used
    # to send the (insecure) authenticated requests.
    request = connection._create_request()
    assert request._http_scheme == constants.HTTP


def _statement_uri(query_id, token):
    return f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}/{query_id}/{token}"


@httprettified
def test_cursor_close_does_not_cancel_finished_update_query():
    """Regression test for https://github.com/trinodb/trino-python-client/issues/601

    An update statement (INSERT/UPDATE/DELETE) reports its affected row count as
    a single synthetic row while Trino still returns a final nextUri. Closing the
    cursor without fetching must drain that nextUri instead of issuing a DELETE,
    otherwise the already-completed statement is reported as USER_CANCELED.
    """
    query_id = "20210817_140827_00000_arvdv"
    statement_path = f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}"

    post_response = {
        "id": query_id,
        "nextUri": _statement_uri(query_id, 1),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{query_id}",
        "stats": {"state": "QUEUED"},
    }
    # The update-count row arrives together with a still-present nextUri.
    update_response = {
        "id": query_id,
        "nextUri": _statement_uri(query_id, 2),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{query_id}",
        "updateType": "INSERT",
        "updateCount": 1000,
        "columns": [{
            "name": "rows",
            "type": "bigint",
            "typeSignature": {"rawType": "bigint", "arguments": [], "typeArguments": []},
        }],
        "data": [[1000]],
        "stats": {"state": "FINISHED"},
    }
    # Final response transitions the query to a terminal state (no nextUri).
    # Trino keeps repeating updateType/updateCount on the trailing pages.
    final_response = {
        "id": query_id,
        "infoUri": f"{SERVER_ADDRESS}/query.html?{query_id}",
        "updateType": "INSERT",
        "updateCount": 1000,
        "columns": update_response["columns"],
        "stats": {"state": "FINISHED"},
    }

    httpretty.register_uri(method=httpretty.POST, uri=statement_path, body=json.dumps(post_response))
    httpretty.register_uri(method=httpretty.GET, uri=_statement_uri(query_id, 1), body=json.dumps(update_response))
    httpretty.register_uri(method=httpretty.GET, uri=_statement_uri(query_id, 2), body=json.dumps(final_response))
    httpretty.register_uri(method=httpretty.DELETE, uri=_statement_uri(query_id, 2), status=204)

    with connect("coordinator", user="test", http_scheme=constants.HTTPS) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO some_table VALUES (1), (2), (3)")
        # execute() must have drained the query to a terminal state.
        assert cur._query.finished is True
        assert cur.rowcount == 1000
        cur.close()

    delete_requests = [r for r in httpretty.latest_requests() if r.method == "DELETE"]
    assert delete_requests == [], "closing a finished update query must not issue a cancel"


@httprettified
def test_cursor_close_cancels_unfinished_query():
    """Closing a cursor whose result set has not been fully consumed must still
    cancel the running query so the server can free its resources.
    """
    query_id = "20210817_140827_00000_arvdv"
    statement_path = f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}"

    post_response = {
        "id": query_id,
        "nextUri": _statement_uri(query_id, 1),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{query_id}",
        "stats": {"state": "QUEUED"},
    }
    # A SELECT that returns a first page of data with more still pending.
    data_response = {
        "id": query_id,
        "nextUri": _statement_uri(query_id, 2),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{query_id}",
        "columns": [{
            "name": "x",
            "type": "bigint",
            "typeSignature": {"rawType": "bigint", "arguments": [], "typeArguments": []},
        }],
        "data": [[1]],
        "stats": {"state": "RUNNING"},
    }

    httpretty.register_uri(method=httpretty.POST, uri=statement_path, body=json.dumps(post_response))
    httpretty.register_uri(method=httpretty.GET, uri=_statement_uri(query_id, 1), body=json.dumps(data_response))
    httpretty.register_uri(method=httpretty.DELETE, uri=_statement_uri(query_id, 2), status=204)

    with connect("coordinator", user="test", http_scheme=constants.HTTPS) as conn:
        cur = conn.cursor()
        cur.execute("SELECT x FROM some_table")
        assert cur._query.finished is False
        cur.close()

    delete_requests = [r for r in httpretty.latest_requests() if r.method == "DELETE"]
    assert len(delete_requests) == 1, "closing an unfinished query must cancel it"
