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


from requests_kerberos.exceptions import KerberosExchangeError
from trino.client import TrinoQuery, TrinoRequest, TrinoResult
from trino.auth import KerberosAuthentication
from trino import constants
import trino.exceptions


"""
This is the response to the first HTTP request (a POST) from an actual
Trino session. It is deliberately not truncated to document such response
and allow to use it for other tests.
To get some HTTP response, set logging level to DEBUG with
``logging.basicConfig(level=logging.DEBUG)`` or
``trino.client.logger.setLevel(logging.DEBUG)``.

::
    from trino import dbapi

    >>> import logging
    >>> import trino.client
    >>> trino.client.logger.setLevel(logging.DEBUG)
    >>> conn = dbapi.Connection('localhost', 8080, 'ggreg', 'test')
    >>> cur = conn.cursor()
    >>> res = cur.execute('select * from system.runtime.nodes')

"""
RESP_DATA_POST_0 = {
    "nextUri": "coordinator:8080/v1/statement/20210817_140827_00000_arvdv/1",
    "id": "20210817_140827_00000_arvdv",
    "taskDownloadUris": [],
    "infoUri": "http://coordinator:8080/query.html?20210817_140827_00000_arvdv",
    "stats": {
        "scheduled": False,
        "runningSplits": 0,
        "processedRows": 0,
        "queuedSplits": 0,
        "processedBytes": 0,
        "state": "QUEUED",
        "completedSplits": 0,
        "queued": True,
        "cpuTimeMillis": 0,
        "totalSplits": 0,
        "nodes": 0,
        "userTimeMillis": 0,
        "wallTimeMillis": 0,
    },
}

"""
This is the response to the second HTTP request (a GET) from an actual
Trino session. It is deliberately not truncated to document such response
and allow to use it for other tests. After doing the steps above, do:

::
    >>> cur.fetchall()

"""
RESP_DATA_GET_0 = {
    "id": "20210817_140827_00000_arvdv",
    "nextUri": "coordinator:8080/v1/statement/20210817_140827_00000_arvdv/2",
    "data": [
        ["UUID-0", "http://worker0:8080", "0.157", False, "active"],
        ["UUID-1", "http://worker1:8080", "0.157", False, "active"],
        ["UUID-2", "http://worker2:8080", "0.157", False, "active"],
    ],
    "columns": [
        {
            "name": "node_id",
            "type": "varchar",
            "typeSignature": {
                "typeArguments": [],
                "arguments": [{"kind": "LONG_LITERAL", "value": 2147483647}],
                "literalArguments": [],
                "rawType": "varchar",
            },
        },
        {
            "name": "http_uri",
            "type": "varchar",
            "typeSignature": {
                "typeArguments": [],
                "arguments": [{"kind": "LONG_LITERAL", "value": 2147483647}],
                "literalArguments": [],
                "rawType": "varchar",
            },
        },
        {
            "name": "node_version",
            "type": "varchar",
            "typeSignature": {
                "typeArguments": [],
                "arguments": [{"kind": "LONG_LITERAL", "value": 2147483647}],
                "literalArguments": [],
                "rawType": "varchar",
            },
        },
        {
            "name": "coordinator",
            "type": "boolean",
            "typeSignature": {
                "typeArguments": [],
                "arguments": [],
                "literalArguments": [],
                "rawType": "boolean",
            },
        },
        {
            "name": "state",
            "type": "varchar",
            "typeSignature": {
                "typeArguments": [],
                "arguments": [{"kind": "LONG_LITERAL", "value": 2147483647}],
                "literalArguments": [],
                "rawType": "varchar",
            },
        },
    ],
    "taskDownloadUris": [],
    "partialCancelUri": "http://localhost:8080/v1/stage/20210817_140827_00000_arvdv.0",  # NOQA: E501
    "stats": {
        "nodes": 2,
        "processedBytes": 880,
        "scheduled": True,
        "completedSplits": 2,
        "userTimeMillis": 0,
        "state": "RUNNING",
        "rootStage": {
            "nodes": 1,
            "done": False,
            "processedBytes": 1044,
            "subStages": [
                {
                    "nodes": 1,
                    "done": True,
                    "processedBytes": 880,
                    "subStages": [],
                    "completedSplits": 1,
                    "userTimeMillis": 0,
                    "state": "FINISHED",
                    "cpuTimeMillis": 3,
                    "runningSplits": 0,
                    "totalSplits": 1,
                    "processedRows": 8,
                    "stageId": "1",
                    "queuedSplits": 0,
                    "wallTimeMillis": 27,
                }
            ],
            "completedSplits": 1,
            "userTimeMillis": 0,
            "state": "RUNNING",
            "cpuTimeMillis": 1,
            "runningSplits": 0,
            "totalSplits": 1,
            "processedRows": 8,
            "stageId": "0",
            "queuedSplits": 0,
            "wallTimeMillis": 9,
        },
        "queued": False,
        "cpuTimeMillis": 3,
        "runningSplits": 0,
        "totalSplits": 2,
        "processedRows": 8,
        "queuedSplits": 0,
        "wallTimeMillis": 36,
    },
    "infoUri": "http://coordinator:8080/query.html?20210817_140827_00000_arvdv",  # NOQA: E501
}

RESP_ERROR_GET_0 = {
    "error": {
        "errorCode": 1,
        "errorLocation": {"columnNumber": 15, "lineNumber": 1},
        "errorName": "SYNTAX_ERROR",
        "errorType": "USER_ERROR",
        "failureInfo": {
            "errorLocation": {"columnNumber": 15, "lineNumber": 1},
            "message": "line 1:15: Schema must be specified "
            "when session schema is not set",
            "stack": [
                "io.trino.sql.analyzer.SemanticExceptions.semanticException(SemanticExceptions.java:48)",
                "io.trino.sql.analyzer.SemanticExceptions.semanticException(SemanticExceptions.java:43)",
                "io.trino.metadata.MetadataUtil.lambda$createQualifiedObjectName$3(MetadataUtil.java:152)",
                "java.base/java.util.Optional.orElseThrow(Optional.java:408)",
                "io.trino.metadata.MetadataUtil.createQualifiedObjectName(MetadataUtil.java:151)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.visitTable(StatementAnalyzer.java:1298)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.visitTable(StatementAnalyzer.java:361)",
                "io.trino.sql.tree.Table.accept(Table.java:53)",
                "io.trino.sql.tree.AstVisitor.process(AstVisitor.java:27)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.process(StatementAnalyzer.java:378)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.analyzeFrom(StatementAnalyzer.java:3182)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.visitQuerySpecification(StatementAnalyzer.java:1954)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.visitQuerySpecification(StatementAnalyzer.java:361)",
                "io.trino.sql.tree.QuerySpecification.accept(QuerySpecification.java:155)",
                "io.trino.sql.tree.AstVisitor.process(AstVisitor.java:27)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.process(StatementAnalyzer.java:378)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.process(StatementAnalyzer.java:388)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.visitQuery(StatementAnalyzer.java:1168)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.visitQuery(StatementAnalyzer.java:361)",
                "io.trino.sql.tree.Query.accept(Query.java:107)",
                "io.trino.sql.tree.AstVisitor.process(AstVisitor.java:27)",
                "io.trino.sql.analyzer.StatementAnalyzer$Visitor.process(StatementAnalyzer.java:378)",
                "io.trino.sql.analyzer.StatementAnalyzer.analyze(StatementAnalyzer.java:341)",
                "io.trino.sql.analyzer.Analyzer.analyze(Analyzer.java:91)",
                "io.trino.sql.analyzer.Analyzer.analyze(Analyzer.java:83)",
                "io.trino.execution.SqlQueryExecution.analyze(SqlQueryExecution.java:269)",
                "io.trino.execution.SqlQueryExecution.\u003Cinit\u003E(SqlQueryExecution.java:190)",
                "io.trino.execution.SqlQueryExecution$SqlQueryExecutionFactory.createQueryExecution(SqlQueryExecution.java:806)",  # NOQA: E501
                "io.trino.dispatcher.LocalDispatchQueryFactory.lambda$createDispatchQuery$0(LocalDispatchQueryFactory.java:132)",  # NOQA: E501
                "io.trino.$gen.Trino_360____20210817_140756_2.call(Unknown Source)",
                "com.google.common.util.concurrent.TrustedListenableFutureTask$TrustedFutureInterruptibleTask.runInterruptibly(TrustedListenableFutureTask.java:125)",  # NOQA: E501
                "com.google.common.util.concurrent.InterruptibleTask.run(InterruptibleTask.java:69)",
                "com.google.common.util.concurrent.TrustedListenableFutureTask.run(TrustedListenableFutureTask.java:78)",  # NOQA: E501
                "java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)",
                "java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)",
                "java.base/java.lang.Thread.run(Thread.java:829)",
            ],
            "suppressed": [],
            "type": "io.trino.spi.TrinoException",
        },
        "message": "line 1:15: Schema must be specified when session schema is not set",
    },
    "id": "20210817_140827_00000_arvdv",
    "infoUri": "http://localhost:8080/query.html?20210817_140827_00000_arvdv",
    "stats": {
        "completedSplits": 0,
        "cpuTimeMillis": 0,
        "nodes": 0,
        "processedBytes": 0,
        "processedRows": 0,
        "queued": False,
        "queuedSplits": 0,
        "runningSplits": 0,
        "scheduled": False,
        "state": "FAILED",
        "totalSplits": 0,
        "userTimeMillis": 0,
        "wallTimeMillis": 0,
    },
    "taskDownloadUris": [],
}


def get_json_post_0(self):
    return RESP_DATA_POST_0


def get_json_get_0(self):
    return RESP_DATA_GET_0


def get_json_get_error_0(self):
    return RESP_ERROR_GET_0


def test_trino_initial_request(monkeypatch):
    monkeypatch.setattr(TrinoRequest.http.Response, "json", get_json_post_0)

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

    assert status.next_uri == RESP_DATA_POST_0["nextUri"]
    assert status.id == RESP_DATA_POST_0["id"]


class ArgumentsRecorder(object):
    def __init__(self):
        # Prevent functools.wraps from complaining when it decorates the
        # instance.
        self.__name__ = "ArgumentsRecorder"
        self.args = None
        self.kwargs = None

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def test_request_headers(monkeypatch):
    post_recorder = ArgumentsRecorder()
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_recorder)

    get_recorder = ArgumentsRecorder()
    monkeypatch.setattr(TrinoRequest.http.Session, "get", get_recorder)

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
    assert_headers(post_recorder.kwargs["headers"])

    req.get("URL")
    assert_headers(get_recorder.kwargs["headers"])


def test_additional_request_post_headers(monkeypatch):
    """
    Tests that the `TrinoRequest.post` function can take addtional headers
    and that it combines them with the existing ones to perform the request.
    """
    post_recorder = ArgumentsRecorder()
    monkeypatch.setattr(TrinoRequest.http.Session, "post", post_recorder)

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
    assert post_recorder.kwargs['headers'] == combined_headers


def test_request_invalid_http_headers():
    with pytest.raises(ValueError) as value_error:
        TrinoRequest(
            host="coordinator",
            port=8080,
            user="test",
            http_headers={constants.HEADER_USER: "invalid_header"},
        )
    assert str(value_error.value).startswith("cannot override reserved HTTP header")


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


def test_trino_fetch_request(monkeypatch):
    monkeypatch.setattr(TrinoRequest.http.Response, "json", get_json_get_0)

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

    assert status.next_uri == RESP_DATA_GET_0["nextUri"]
    assert status.id == RESP_DATA_GET_0["id"]
    assert status.rows == RESP_DATA_GET_0["data"]


def test_trino_fetch_error(monkeypatch):
    monkeypatch.setattr(TrinoRequest.http.Response, "json", get_json_get_error_0)

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


def test_trino_query_response_headers():
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
            return get_json_post_0(self)

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
