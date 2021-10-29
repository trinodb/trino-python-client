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

from requests import Session
from unittest.mock import patch
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
