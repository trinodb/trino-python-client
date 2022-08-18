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
from unittest.mock import patch, MagicMock

import httpretty
from httpretty import httprettified
from requests import Session

from tests.unit.oauth_test_utils import _post_statement_requests, _get_token_requests, RedirectHandler, \
    GetTokenCallback, REDIRECT_RESOURCE, TOKEN_RESOURCE, PostStatementCallback, SERVER_ADDRESS
from trino import constants
from trino.auth import OAuth2Authentication, OAuth2TokenCache
from trino.dbapi import connect


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
def test_token_retrieved_once_per_auth_instance(sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)

    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}:8080{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

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

    assert len(_get_token_requests(challenge_id)) == 2


@httprettified
def test_custom_token_cache_is_invoked(sample_post_response_data):
    host = "coordinator"
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)

    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}:8080{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    custom_cache = MagicMock(OAuth2TokenCache)
    custom_cache.get_token_from_cache = MagicMock(side_effect=[None, token, token, token])
    custom_cache.store_token_to_cache = MagicMock()

    with connect(
            host,
            user="test",
            auth=OAuth2Authentication(redirect_auth_url_handler=redirect_handler, cache=custom_cache),
            http_scheme=constants.HTTPS
    ) as conn:
        conn.cursor().execute("SELECT 1")
        conn.cursor().execute("SELECT 2")
        conn.cursor().execute("SELECT 3")

    assert len(_get_token_requests(challenge_id)) == 1
    custom_cache.get_token_from_cache.assert_called_with(host)
    assert custom_cache.get_token_from_cache.call_count == 4
    custom_cache.store_token_to_cache.assert_called_with(host, token)
    assert custom_cache.store_token_to_cache.call_count == 1


@httprettified
def test_token_retrieved_once_when_authentication_instance_is_shared(sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)

    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}:8080{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

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
def test_token_retrieved_once_when_multithreaded(sample_post_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)

    # bind post statement
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}:8080{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

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
    # WHEN
    client_tags = ["TAG1", "TAG2"]
    with connect("sample_trino_cluster:443", client_tags=client_tags) as conn:
        conn.cursor().execute("SOME FAKE QUERY")

    # THEN
    _, passed_client_tags = mock_client.ClientSession.call_args
    assert passed_client_tags["client_tags"] == client_tags
