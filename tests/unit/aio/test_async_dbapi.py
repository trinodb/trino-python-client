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

import httpx2
import pytest

import trino.aio
import trino.exceptions
from tests.unit.mock_http import MockTrinoServer
from trino import constants
from trino.transaction import IsolationLevel

_QUERY_ID = "20210817_140827_00000_arvdv"
SERVER_ADDRESS = "https://coordinator"

_BIGINT_COLUMNS = [{
    "name": "x",
    "type": "bigint",
    "typeSignature": {"rawType": "bigint", "arguments": [], "typeArguments": []},
}]


def _statement_path(token):
    return f"{constants.URL_STATEMENT_PATH}/{_QUERY_ID}/{token}"


def _statement_uri(token):
    return f"{SERVER_ADDRESS}{_statement_path(token)}"


def _select_query_server(rows_pages):
    """A mock coordinator serving a SELECT query returning the given pages of rows."""
    server = MockTrinoServer()
    server.register("POST", constants.URL_STATEMENT_PATH, json={
        "id": _QUERY_ID,
        "nextUri": _statement_uri(1),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{_QUERY_ID}",
        "stats": {"state": "QUEUED"},
    })
    for index, page in enumerate(rows_pages, start=1):
        response = {
            "id": _QUERY_ID,
            "infoUri": f"{SERVER_ADDRESS}/query.html?{_QUERY_ID}",
            "columns": _BIGINT_COLUMNS,
            "data": page,
            "stats": {"state": "RUNNING"},
        }
        if index < len(rows_pages):
            response["nextUri"] = _statement_uri(index + 1)
        else:
            response["stats"] = {"state": "FINISHED"}
        server.register("GET", _statement_path(index), json=response)
    return server


def _connect(server, **kwargs):
    return trino.aio.connect(
        "coordinator",
        user="test",
        http_scheme=constants.HTTPS,
        http_session=server.async_client(),
        **kwargs,
    )


async def test_fetchall():
    server = _select_query_server([[[1]], [[2], [3]]])
    async with _connect(server) as conn:
        cur = conn.cursor()
        await cur.execute("SELECT x FROM some_table")
        rows = await cur.fetchall()
        assert rows == [[1], [2], [3]]
        assert cur.query_id == _QUERY_ID
        assert cur.description[0].name == "x"
        assert cur.description[0].type_code == "bigint"


async def test_async_iteration():
    server = _select_query_server([[[1]], [[2], [3]]])
    async with _connect(server) as conn:
        cur = conn.cursor()
        await cur.execute("SELECT x FROM some_table")
        rows = [row async for row in cur]
        assert rows == [[1], [2], [3]]


async def test_fetchone_and_fetchmany():
    server = _select_query_server([[[1], [2], [3]]])
    async with _connect(server) as conn:
        cur = conn.cursor()
        await cur.execute("SELECT x FROM some_table")
        assert await cur.fetchone() == [1]
        assert await cur.fetchmany(2) == [[2], [3]]
        assert await cur.fetchmany(2) == []


async def test_cursor_close_cancels_unfinished_query():
    server = MockTrinoServer()
    server.register("POST", constants.URL_STATEMENT_PATH, json={
        "id": _QUERY_ID,
        "nextUri": _statement_uri(1),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{_QUERY_ID}",
        "stats": {"state": "QUEUED"},
    })
    server.register("GET", _statement_path(1), json={
        "id": _QUERY_ID,
        "nextUri": _statement_uri(2),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{_QUERY_ID}",
        "columns": _BIGINT_COLUMNS,
        "data": [[1]],
        "stats": {"state": "RUNNING"},
    })
    server.register("DELETE", _statement_path(2), status=204, text="")

    async with _connect(server) as conn:
        cur = conn.cursor()
        await cur.execute("SELECT x FROM some_table")
        assert cur._query.finished is False
        await cur.close()

    assert len(server.requests(method="DELETE")) == 1


async def test_spooled_protocol_with_inline_segment():
    encoded_rows = base64.b64encode(b"[[1], [2]]").decode("utf8")
    server = MockTrinoServer()
    server.register("POST", constants.URL_STATEMENT_PATH, json={
        "id": _QUERY_ID,
        "nextUri": _statement_uri(1),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{_QUERY_ID}",
        "stats": {"state": "QUEUED"},
    })
    server.register("GET", _statement_path(1), json={
        "id": _QUERY_ID,
        "infoUri": f"{SERVER_ADDRESS}/query.html?{_QUERY_ID}",
        "columns": _BIGINT_COLUMNS,
        "data": {
            "encoding": "json",
            "segments": [{
                "type": "inline",
                "metadata": {"segmentSize": "10"},
                "data": encoded_rows,
            }],
        },
        "stats": {"state": "FINISHED"},
    })

    async with _connect(server, encoding="json") as conn:
        cur = conn.cursor()
        await cur.execute("SELECT x FROM some_table")
        rows = await cur.fetchall()
        assert rows == [[1], [2]]


async def test_spooled_protocol_downloads_segment_and_acknowledges(monkeypatch):
    segment_rows = b"[[1], [2], [3]]"

    server = MockTrinoServer()
    server.register("POST", constants.URL_STATEMENT_PATH, json={
        "id": _QUERY_ID,
        "nextUri": _statement_uri(1),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{_QUERY_ID}",
        "stats": {"state": "QUEUED"},
    })
    server.register("GET", _statement_path(1), json={
        "id": _QUERY_ID,
        "infoUri": f"{SERVER_ADDRESS}/query.html?{_QUERY_ID}",
        "columns": _BIGINT_COLUMNS,
        "data": {
            "encoding": "json",
            "segments": [{
                "type": "spooled",
                "metadata": {"segmentSize": str(len(segment_rows))},
                "uri": "https://storage.example.com/segments/1",
                "ackUri": "https://coordinator/v1/spooled/ack/1",
                "headers": {"X-Trino-Spooling-Token": ["token-abc"]},
            }],
        },
        "stats": {"state": "FINISHED"},
    })

    def segment_handler(request):
        assert request.headers["X-Trino-Spooling-Token"] == "token-abc"
        return httpx2.Response(200, content=segment_rows)

    server.register("GET", "/segments/1", segment_handler)
    server.register("GET", "/v1/spooled/ack/1", status=204, text="")

    # Spooled segment downloads run over a fresh unauthenticated client built
    # by create_http_client; route those clients through the mock transport.
    original = trino.aio.AsyncTrinoRequest.create_http_client.__func__

    def create_with_mock_transport(cls, **kwargs):
        kwargs.setdefault("transport", server.transport())
        return original(cls, **kwargs)

    monkeypatch.setattr(
        trino.aio.AsyncTrinoRequest, "create_http_client", classmethod(create_with_mock_transport))

    async with _connect(server, encoding="json") as conn:
        cur = conn.cursor()
        await cur.execute("SELECT x FROM some_table")
        rows = await cur.fetchall()
        assert rows == [[1], [2], [3]]

    # conn.close() drained the acknowledgment task.
    ack_requests = server.requests(method="GET", path="/v1/spooled/ack/1")
    assert len(ack_requests) == 1


async def test_transactions_not_supported():
    with pytest.raises(trino.exceptions.NotSupportedError):
        trino.aio.connect("coordinator", user="test", isolation_level=IsolationLevel.SERIALIZABLE)

    conn = trino.aio.connect("coordinator", user="test")
    with pytest.raises(trino.exceptions.NotSupportedError):
        conn.start_transaction()
    await conn.close()


async def test_http_session_is_defaulted_when_not_specified():
    async with trino.aio.connect("coordinator", user="test") as conn:
        assert isinstance(conn._http_session, httpx2.AsyncClient)
        assert conn._create_request()._http_session is conn._http_session
