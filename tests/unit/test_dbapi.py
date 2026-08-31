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
import threading
import uuid
from unittest.mock import patch

import httpx2
import pytest

import trino.exceptions
from tests.unit.mock_http import MockTrinoServer
from tests.unit.oauth_test_utils import _get_token_requests
from tests.unit.oauth_test_utils import _post_statement_requests
from tests.unit.oauth_test_utils import GetTokenCallback
from tests.unit.oauth_test_utils import PostStatementCallback
from tests.unit.oauth_test_utils import REDIRECT_RESOURCE
from tests.unit.oauth_test_utils import RedirectHandler
from tests.unit.oauth_test_utils import SERVER_ADDRESS
from tests.unit.oauth_test_utils import TOKEN_PATH
from tests.unit.oauth_test_utils import TOKEN_RESOURCE
from trino import constants
from trino.auth import BasicAuthentication
from trino.auth import OAuth2Authentication
from trino.dbapi import Binary
from trino.dbapi import connect
from trino.dbapi import Connection
from trino.dbapi import Cursor

_QUERY_ID = "20210817_140827_00000_arvdv"


def _finished_statement_response():
    """A POST /v1/statement response without a nextUri: the query is
    immediately in a terminal state, so no follow-up requests are needed."""
    return {
        "id": _QUERY_ID,
        "infoUri": f"{SERVER_ADDRESS}/query.html?{_QUERY_ID}",
        "stats": {"state": "FINISHED"},
    }


def test_http_session_is_correctly_passed_in(trino_server):
    trino_server.register("POST", constants.URL_STATEMENT_PATH, json=_finished_statement_response())
    test_session = trino_server.client()

    with connect("https://sample_trino_cluster:443", user="test", http_session=test_session) as conn:
        conn.cursor().execute("SOME FAKE QUERY")
        assert conn._http_session is test_session

    assert len(trino_server.requests(method="POST", path=constants.URL_STATEMENT_PATH)) == 1


def test_http_session_is_defaulted_when_not_specified():
    with connect("sample_trino_cluster:443") as conn:
        assert isinstance(conn._http_session, httpx2.Client)
        assert conn._create_request()._http_session is conn._http_session


def test_token_retrieved_once_per_auth_instance(sample_post_response_data, sample_get_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    server = MockTrinoServer()

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    get_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_get_response_data)

    # bind post statement to submit query
    server.register("POST", constants.URL_STATEMENT_PATH, post_statement_callback)

    # bind get statement for result retrieval
    server.register("GET", f"{constants.URL_STATEMENT_PATH}/{_QUERY_ID}/1", get_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", get_token_callback)

    redirect_handler = RedirectHandler()

    with connect(
            "coordinator",
            user="test",
            auth=OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
            http_scheme=constants.HTTPS,
            http_session=server.client(),
    ) as conn:
        conn.cursor().execute("SELECT 1")
        conn.cursor().execute("SELECT 2")
        conn.cursor().execute("SELECT 3")

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", get_token_callback)

    redirect_handler = RedirectHandler()

    with connect(
            "coordinator",
            user="test",
            auth=OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
            http_scheme=constants.HTTPS,
            http_session=server.client(),
    ) as conn2:
        conn2.cursor().execute("SELECT 1")
        conn2.cursor().execute("SELECT 2")
        conn2.cursor().execute("SELECT 3")

    assert len(_get_token_requests(server, challenge_id)) == 1


def test_token_retrieved_once_when_authentication_instance_is_shared(sample_post_response_data,
                                                                     sample_get_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    server = MockTrinoServer()

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    get_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_get_response_data)

    # bind post statement to submit query
    server.register("POST", constants.URL_STATEMENT_PATH, post_statement_callback)

    # bind get statement for result retrieval
    server.register("GET", f"{constants.URL_STATEMENT_PATH}/{_QUERY_ID}/1", get_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", get_token_callback)

    redirect_handler = RedirectHandler()

    authentication = OAuth2Authentication(redirect_auth_url_handler=redirect_handler)

    with connect(
            "coordinator",
            user="test",
            auth=authentication,
            http_scheme=constants.HTTPS,
            http_session=server.client(),
    ) as conn:
        conn.cursor().execute("SELECT 1")
        conn.cursor().execute("SELECT 2")
        conn.cursor().execute("SELECT 3")

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", get_token_callback)

    with connect(
            "coordinator",
            user="test",
            auth=authentication,
            http_scheme=constants.HTTPS,
            http_session=server.client(),
    ) as conn2:
        conn2.cursor().execute("SELECT 1")
        conn2.cursor().execute("SELECT 2")
        conn2.cursor().execute("SELECT 3")

    assert len(_post_statement_requests(server)) == 7
    assert len(_get_token_requests(server, challenge_id)) == 1


def test_token_retrieved_once_when_multithreaded(sample_post_response_data, sample_get_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    server = MockTrinoServer()

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    get_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_get_response_data)

    # bind post statement to submit query
    server.register("POST", constants.URL_STATEMENT_PATH, post_statement_callback)

    # bind get statement for result retrieval
    server.register("GET", f"{constants.URL_STATEMENT_PATH}/{_QUERY_ID}/1", get_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    server.register("GET", f"/{TOKEN_PATH}/{challenge_id}", get_token_callback)

    redirect_handler = RedirectHandler()

    authentication = OAuth2Authentication(redirect_auth_url_handler=redirect_handler)

    conn = connect(
        "coordinator",
        user="test",
        auth=authentication,
        http_scheme=constants.HTTPS,
        http_session=server.client(),
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

    assert len(_get_token_requests(server, challenge_id)) == 1


def test_tags_are_set_when_specified():
    client_tags = ["TAG1", "TAG2"]
    with connect("sample_trino_cluster:443", client_tags=client_tags) as conn:
        assert conn._client_session.client_tags == client_tags


def test_role_is_set_when_specified():
    roles = {"system": "finance"}
    with connect("sample_trino_cluster:443", roles=roles) as conn:
        assert conn._client_session.roles == {"system": "ROLE{finance}"}


def test_hostname_parsing():
    https_server_with_port = Connection("https://mytrinoserver.domain:9999")
    assert https_server_with_port.host == "mytrinoserver.domain"
    assert https_server_with_port.port == 9999
    assert https_server_with_port.http_scheme == constants.HTTPS

    https_server_without_port = Connection("https://mytrinoserver.domain")
    assert https_server_without_port.host == "mytrinoserver.domain"
    assert https_server_without_port.port == constants.DEFAULT_TLS_PORT
    assert https_server_without_port.http_scheme == constants.HTTPS

    http_server_with_port = Connection("http://mytrinoserver.domain:9999")
    assert http_server_with_port.host == "mytrinoserver.domain"
    assert http_server_with_port.port == 9999
    assert http_server_with_port.http_scheme == constants.HTTP

    http_server_without_port = Connection("http://mytrinoserver.domain")
    assert http_server_without_port.host == "mytrinoserver.domain"
    assert http_server_without_port.port == constants.DEFAULT_PORT
    assert http_server_without_port.http_scheme == constants.HTTP

    http_server_with_path = Connection("http://mytrinoserver.domain/some_path")
    assert http_server_with_path.host == "mytrinoserver.domain/some_path"
    assert http_server_with_path.port == constants.DEFAULT_PORT
    assert http_server_with_path.http_scheme == constants.HTTP

    only_hostname = Connection("mytrinoserver.domain")
    assert only_hostname.host == "mytrinoserver.domain"
    assert only_hostname.port == constants.DEFAULT_PORT
    assert only_hostname.http_scheme == constants.HTTP

    only_hostname_with_path = Connection("mytrinoserver.domain/some_path")
    assert only_hostname_with_path.host == "mytrinoserver.domain/some_path"
    assert only_hostname_with_path.port == constants.DEFAULT_PORT
    assert only_hostname_with_path.http_scheme == constants.HTTP


def test_description_is_none_when_cursor_is_not_executed():
    connection = Connection("sample_trino_cluster:443")
    with connection.cursor() as cursor:
        assert hasattr(cursor, 'description')


@pytest.mark.parametrize(
    "host, port, http_scheme_input_argument, http_scheme_set",
    [
        # Infer from hostname
        ("https://mytrinoserver.domain:9999", None, None, constants.HTTPS),
        ("http://mytrinoserver.domain:9999", None, None, constants.HTTP),
        # Infer from port
        ("mytrinoserver.domain", constants.DEFAULT_TLS_PORT, None, constants.HTTPS),
        ("mytrinoserver.domain", constants.DEFAULT_PORT, None, constants.HTTP),
        # http_scheme parameter has higher precedence than port parameter
        ("mytrinoserver.domain", constants.DEFAULT_TLS_PORT, constants.HTTP, constants.HTTP),
        ("mytrinoserver.domain", constants.DEFAULT_PORT, constants.HTTPS, constants.HTTPS),
        # Set explicitly by http_scheme parameter
        ("mytrinoserver.domain", None, constants.HTTPS, constants.HTTPS),
        # Default
        ("mytrinoserver.domain", None, None, constants.HTTP),
    ],
)
def test_setting_http_scheme(host, port, http_scheme_input_argument, http_scheme_set):
    connection = Connection(host, port, http_scheme=http_scheme_input_argument)
    assert connection.http_scheme == http_scheme_set


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


def _statement_path(query_id, token):
    return f"{constants.URL_STATEMENT_PATH}/{query_id}/{token}"


def _statement_uri(query_id, token):
    return f"{SERVER_ADDRESS}{_statement_path(query_id, token)}"


def test_cursor_close_does_not_cancel_finished_update_query():
    """Regression test for https://github.com/trinodb/trino-python-client/issues/601

    An update statement (INSERT/UPDATE/DELETE) reports its affected row count as
    a single synthetic row while Trino still returns a final nextUri. Closing the
    cursor without fetching must drain that nextUri instead of issuing a DELETE,
    otherwise the already-completed statement is reported as USER_CANCELED.
    """
    query_id = _QUERY_ID

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

    server = MockTrinoServer()
    server.register("POST", constants.URL_STATEMENT_PATH, json=post_response)
    server.register("GET", _statement_path(query_id, 1), json=update_response)
    server.register("GET", _statement_path(query_id, 2), json=final_response)
    server.register("DELETE", _statement_path(query_id, 2), status=204, text="")

    with connect("coordinator", user="test", http_scheme=constants.HTTPS, http_session=server.client()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO some_table VALUES (1), (2), (3)")
        # execute() must have drained the query to a terminal state.
        assert cur._query.finished is True
        assert cur.rowcount == 1000
        cur.close()

    assert server.requests(method="DELETE") == [], "closing a finished update query must not issue a cancel"


def test_cursor_close_cancels_unfinished_query():
    """Closing a cursor whose result set has not been fully consumed must still
    cancel the running query so the server can free its resources.
    """
    query_id = _QUERY_ID

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

    server = MockTrinoServer()
    server.register("POST", constants.URL_STATEMENT_PATH, json=post_response)
    server.register("GET", _statement_path(query_id, 1), json=data_response)
    server.register("DELETE", _statement_path(query_id, 2), status=204, text="")

    with connect("coordinator", user="test", http_scheme=constants.HTTPS, http_session=server.client()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT x FROM some_table")
        assert cur._query.finished is False
        cur.close()

    assert len(server.requests(method="DELETE")) == 1, "closing an unfinished query must cancel it"


@pytest.mark.parametrize(
    "value,expected",
    [
        (b"", b""),
        (b"hello", b"hello"),
        (b"\xff\xfe\x00\x01", b"\xff\xfe\x00\x01"),
        (bytearray(b"abc"), b"abc"),
        (memoryview(b"xyz"), b"xyz"),
        ("hello", b"hello"),
    ]
)
def test_binary(value, expected):
    # Binary() previously called .encode() unconditionally, which raised
    # AttributeError for bytes/bytearray/memoryview input.
    result = Binary(value)
    assert isinstance(result, bytes)
    assert result == expected


@pytest.mark.parametrize("value", [1, None, 1.5, [b"a"]])
def test_binary_rejects_non_bytes_non_str(value):
    with pytest.raises(TypeError):
        Binary(value)


@pytest.mark.parametrize(
    "value,expected",
    [
        (b"", "X''"),
        (b"hello", "X'68656c6c6f'"),
        (b"\xca\xfe\xba\xbe", "X'cafebabe'"),
        (bytearray(b"abc"), "X'616263'"),
        (memoryview(b"abc"), "X'616263'"),
    ]
)
def test_format_prepared_param_binary(value, expected):
    cursor = Cursor.__new__(Cursor)
    assert cursor._format_prepared_param(value) == expected
    # Round trip through Binary(), as SQLAlchemy's _Binary.bind_processor does.
    assert cursor._format_prepared_param(Binary(value)) == expected
