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

from prestodb.client import PrestoRequest
from prestodb import constants
import prestodb.exceptions


"""
This is the response to the first HTTP request (a POST) from an actual
Presto session. It is deliberately not truncated to document such response
and allow to use it for other tests.
To get some HTTP response, set logging level to DEBUG with
``logging.basicConfig(level=logging.DEBUG)`` or
``prestodb.client.logger.setLevel(logging.DEBUG)``.

::
    from prestodb import dbapi

    >>> import logging
    >>> import prestodb.client
    >>> prestodb.client.logger.setLevel(logging.DEBUG)
    >>> conn = dbapi.Connection('localhost', 8080, 'ggreg', 'test')
    >>> cur = conn.cursor()
    >>> res = cur.execute('select * from system.runtime.nodes')

"""
RESP_DATA_POST_0 = {
    'nextUri': 'coordinator:8080/v1/statement/20161115_222658_00040_xtnym/1',
    'id': '20161115_222658_00040_xtnym',
    'taskDownloadUris': [],
    'infoUri': 'http://coordinator:8080/query.html?20161115_222658_00040_xtnym',
    'stats': {
        'scheduled': False,
        'runningSplits': 0,
        'processedRows': 0,
        'queuedSplits': 0,
        'processedBytes': 0,
        'state': 'QUEUED',
        'completedSplits': 0,
        'queued': True,
        'cpuTimeMillis': 0,
        'totalSplits': 0,
        'nodes': 0,
        'userTimeMillis': 0,
        'wallTimeMillis': 0,
    },
}

"""
This is the response to the second HTTP request (a GET) from an actual
Presto session. It is deliberately not truncated to document such response
and allow to use it for other tests. After doing the steps above, do:

::
    >>> cur.fetchall()

"""
RESP_DATA_GET_0 = {
    'id': '20161116_195728_00000_xtnym',
    'nextUri': 'coordinator:8080/v1/statement/20161116_195728_00000_xtnym/2',
    'data': [
        ['UUID-0', 'http://worker0:8080', '0.157', False, 'active'],
        ['UUID-1', 'http://worker1:8080', '0.157', False, 'active'],
        ['UUID-2', 'http://worker2:8080', '0.157', False, 'active'],
    ],
    'columns': [{
        'name': 'node_id',
        'type': 'varchar',
        'typeSignature': {
            'typeArguments': [],
            'arguments': [{
                'kind': 'LONG_LITERAL',
                'value': 2147483647}
            ],
            'literalArguments': [],
            'rawType': 'varchar',
        }
    }, {
        'name': 'http_uri',
        'type': 'varchar',
        'typeSignature': {
            'typeArguments': [],
            'arguments': [{
                'kind': 'LONG_LITERAL',
                'value': 2147483647,
            }],
            'literalArguments': [],
            'rawType': 'varchar',
        }
    }, {
        'name': 'node_version',
        'type': 'varchar',
        'typeSignature': {
            'typeArguments': [],
            'arguments': [{
                'kind': 'LONG_LITERAL',
                'value': 2147483647,
            }],
            'literalArguments': [],
            'rawType': 'varchar',
        }
    }, {
        'name': 'coordinator',
        'type': 'boolean',
        'typeSignature': {
            'typeArguments': [],
            'arguments': [],
            'literalArguments': [],
            'rawType': 'boolean',
        }
    }, {
        'name': 'state',
        'type': 'varchar',
        'typeSignature': {
            'typeArguments': [],
            'arguments': [{
                'kind': 'LONG_LITERAL',
                'value': 2147483647,
            }],
            'literalArguments': [],
            'rawType': 'varchar',
        }
    }],
    'taskDownloadUris': [],
    'partialCancelUri': 'http://localhost:8080/v1/stage/20161116_195728_00000_xtnym.0',  # NOQA
    'stats': {
        'nodes': 2,
        'processedBytes': 880,
        'scheduled': True,
        'completedSplits': 2,
        'userTimeMillis': 0,
        'state': 'RUNNING',
        'rootStage': {
            'nodes': 1,
            'done': False,
            'processedBytes': 1044,
            'subStages': [{
                'nodes': 1,
                'done': True,
                'processedBytes': 880,
                'subStages': [],
                'completedSplits': 1,
                'userTimeMillis': 0,
                'state': 'FINISHED',
                'cpuTimeMillis': 3,
                'runningSplits': 0,
                'totalSplits': 1,
                'processedRows': 8,
                'stageId': '1',
                'queuedSplits': 0,
                'wallTimeMillis': 27,
            }],
            'completedSplits': 1,
            'userTimeMillis': 0,
            'state': 'RUNNING',
            'cpuTimeMillis': 1,
            'runningSplits': 0,
            'totalSplits': 1,
            'processedRows': 8,
            'stageId': '0',
            'queuedSplits': 0,
            'wallTimeMillis': 9,
        },
        'queued': False,
        'cpuTimeMillis': 3,
        'runningSplits': 0,
        'totalSplits': 2,
        'processedRows': 8,
        'queuedSplits': 0,
        'wallTimeMillis': 36,
    },
    'infoUri': 'http://coordinator:8080/query.html?20161116_195728_00000_xtnym',  # NOQA
}

RESP_ERROR_GET_0 = {
    'error': {
        'errorCode': 1,
        'errorLocation': {'columnNumber': 15, 'lineNumber': 1},
        'errorName': 'SYNTAX_ERROR',
        'errorType': 'USER_ERROR',
        'failureInfo': {
            'errorLocation': {'columnNumber': 15, 'lineNumber': 1},
            'message': 'line 1:15: Schema must be specified '
            'when session schema is not set',
            'stack': [
                'com.facebook.presto.metadata.MetadataUtil.lambda$createQualifiedObjectName$2(MetadataUtil.java:133)',
                'java.util.Optional.orElseThrow(Optional.java:290)',
                'com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName(MetadataUtil.java:132)',
                'com.facebook.presto.sql.analyzer.StatementAnalyzer.visitTable(StatementAnalyzer.java:529)',
                'com.facebook.presto.sql.analyzer.StatementAnalyzer.visitTable(StatementAnalyzer.java:166)',
                'com.facebook.presto.sql.tree.Table.accept(Table.java:50)',
                'com.facebook.presto.sql.tree.AstVisitor.process(AstVisitor.java:22)',
                'com.facebook.presto.sql.analyzer.StatementAnalyzer.analyzeFrom(StatementAnalyzer.java:1413)',
                'com.facebook.presto.sql.analyzer.StatementAnalyzer.visitQuerySpecification(StatementAnalyzer.java:670)',
                'com.facebook.presto.sql.analyzer.StatementAnalyzer.visitQuerySpecification(StatementAnalyzer.java:166)',
                'com.facebook.presto.sql.tree.QuerySpecification.accept(QuerySpecification.java:125)',
                'com.facebook.presto.sql.tree.AstVisitor.process(AstVisitor.java:22)',
                'com.facebook.presto.sql.analyzer.StatementAnalyzer.visitQuery(StatementAnalyzer.java:438)',
                'com.facebook.presto.sql.analyzer.StatementAnalyzer.visitQuery(StatementAnalyzer.java:166)',
                'com.facebook.presto.sql.tree.Query.accept(Query.java:92)',
                'com.facebook.presto.sql.tree.AstVisitor.process(AstVisitor.java:22)',
                'com.facebook.presto.sql.analyzer.Analyzer.analyze(Analyzer.java:67)',
                'com.facebook.presto.sql.analyzer.Analyzer.analyze(Analyzer.java:59)',
                'com.facebook.presto.execution.SqlQueryExecution.doAnalyzeQuery(SqlQueryExecution.java:285)',
                'com.facebook.presto.execution.SqlQueryExecution.analyzeQuery(SqlQueryExecution.java:271)',
                'com.facebook.presto.execution.SqlQueryExecution.start(SqlQueryExecution.java:229)',
                'com.facebook.presto.execution.QueuedExecution.lambda$start$1(QueuedExecution.java:62)',
                'java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)',
                'java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)',
                'java.lang.Thread.run(Thread.java:745)',
            ],
            'suppressed': [],
            'type': 'com.facebook.presto.sql.analyzer.SemanticException',
        },
        'message': 'line 1:15: Schema must be specified when session schema is not set',
    },
    'id': '20161116_205844_00002_xtnym',
    'infoUri': 'http://test02.presto.data.facebook.com:7777/query.html?20161116_205844_00002_xtnym',
    'stats': {'completedSplits': 0,
        'cpuTimeMillis': 0,
        'nodes': 0,
        'processedBytes': 0,
        'processedRows': 0,
        'queued': False,
        'queuedSplits': 0,
        'runningSplits': 0,
        'scheduled': False,
        'state': 'FAILED',
        'totalSplits': 0,
        'userTimeMillis': 0,
        'wallTimeMillis': 0,
    },
    'taskDownloadUris': [],
}


def get_json_post_0(self):
    return RESP_DATA_POST_0


def get_json_get_0(self):
    return RESP_DATA_GET_0


def get_json_get_error_0(self):
    return RESP_ERROR_GET_0


def test_presto_initial_request(monkeypatch):
    monkeypatch.setattr(PrestoRequest.http.Response, 'json', get_json_post_0)

    req = PrestoRequest(
        host='coordinator',
        port=8080,
        user='test',
        source='test',
        catalog='test',
        schema='test',
        http_scheme='http',
        session_properties={},
    )

    http_resp = PrestoRequest.http.Response()
    http_resp.status_code = 200
    status = req.process(http_resp)

    assert status.next_uri == RESP_DATA_POST_0['nextUri']
    assert status.id == RESP_DATA_POST_0['id']


class ArgumentsRecorder(object):
    def __init__(self):
        # Prevent functools.wraps from complaining when it decorates the
        # instance.
        self.__name__ = 'ArgumentsRecorder'
        self.args = None
        self.kwargs = None

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def test_request_headers(monkeypatch):
    post_recorder = ArgumentsRecorder()
    monkeypatch.setattr(PrestoRequest.http.Session, 'post', post_recorder)

    get_recorder = ArgumentsRecorder()
    monkeypatch.setattr(PrestoRequest.http.Session, 'get', get_recorder)

    catalog = 'test_catalog'
    schema = 'test_schema'
    user = 'test_user'
    source = 'test_source'
    accept_encoding_header = 'accept-encoding'
    accept_encoding_value = 'identity,deflate,gzip'
    client_info_header = constants.HEADER_PREFIX + 'Client-Info'
    client_info_value = 'some_client_info'

    req = PrestoRequest(
        host='coordinator',
        port=8080,
        user=user,
        source=source,
        catalog=catalog,
        schema=schema,
        http_scheme='http',
        session_properties={},
        http_headers={
            accept_encoding_header: accept_encoding_value,
            client_info_header: client_info_value,
        },
    )

    def assert_headers(headers):
        assert headers[constants.HEADER_CATALOG] == catalog
        assert headers[constants.HEADER_SCHEMA] == schema
        assert headers[constants.HEADER_SOURCE] == source
        assert headers[constants.HEADER_USER] == user
        assert headers[constants.HEADER_SESSION] == ''
        assert headers[accept_encoding_header] == accept_encoding_value
        assert headers[client_info_header] == client_info_value
        assert len(headers.keys()) == 7

    req.post('URL')
    assert_headers(post_recorder.kwargs['headers'])

    req.get('URL')
    assert_headers(get_recorder.kwargs['headers'])


def test_request_invalid_http_headers():
    with pytest.raises(ValueError) as value_error:
        PrestoRequest(
            host='coordinator',
            port=8080,
            user='test',
            http_headers={constants.HEADER_USER: 'invalid_header'},
        )
    assert str(value_error.value).startswith('cannot override reserved HTTP header')


def test_request_timeout():
    timeout = 0.1
    http_scheme = 'http'
    host = 'coordinator'
    port = 8080
    url = http_scheme + '://' + host + ':' + str(port) + constants.URL_STATEMENT_PATH

    def long_call(request, uri, headers):
        time.sleep(timeout * 2)
        return (200, headers, "delayed success")

    httpretty.enable()
    for method in [httpretty.POST, httpretty.GET]:
        httpretty.register_uri(method, url, body=long_call)

    # timeout without retry
    for request_timeout in [timeout, (timeout, timeout)]:
        req = PrestoRequest(
            host=host,
            port=port,
            user='test',
            http_scheme=http_scheme,
            max_attempts=1,
            request_timeout=request_timeout,
        )

        with pytest.raises(requests.exceptions.Timeout):
            req.get(url)

        with pytest.raises(requests.exceptions.Timeout):
            req.post('select 1')

    httpretty.disable()
    httpretty.reset()


def test_presto_fetch_request(monkeypatch):
    monkeypatch.setattr(PrestoRequest.http.Response, 'json', get_json_get_0)

    req = PrestoRequest(
        host='coordinator',
        port=8080,
        user='test',
        source='test',
        catalog='test',
        schema='test',
        http_scheme='http',
        session_properties={},
    )

    http_resp = PrestoRequest.http.Response()
    http_resp.status_code = 200
    status = req.process(http_resp)

    assert status.next_uri == RESP_DATA_GET_0['nextUri']
    assert status.id == RESP_DATA_GET_0['id']
    assert status.rows == RESP_DATA_GET_0['data']


def test_presto_fetch_error(monkeypatch):
    monkeypatch.setattr(
        PrestoRequest.http.Response,
        'json',
        get_json_get_error_0,
    )

    req = PrestoRequest(
        host='coordinator',
        port=8080,
        user='test',
        source='test',
        catalog='test',
        schema='test',
        http_scheme='http',
        session_properties={},
    )

    http_resp = PrestoRequest.http.Response()
    http_resp.status_code = 200
    with pytest.raises(prestodb.exceptions.PrestoUserError):
        req.process(http_resp)
