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

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="session")
def sample_post_response_data():
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

    yield {
        "nextUri": "https://coordinator:8080/v1/statement/20210817_140827_00000_arvdv/1",
        "id": "20210817_140827_00000_arvdv",
        "taskDownloadUris": [],
        "infoUri": "https://coordinator:8080/query.html?20210817_140827_00000_arvdv",
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


@pytest.fixture(scope="session")
def sample_get_response_data():
    """
    This is the response to the second HTTP request (a GET) from an actual
    Trino session. It is deliberately not truncated to document such response
    and allow to use it for other tests. After doing the steps above, do:

    ::
        >>> cur.fetchall()

    """
    yield {
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


@pytest.fixture(scope="session")
def sample_get_error_response_data():
    yield {
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
                    "io.trino.sql.analyzer.StatementAnalyzer$Visitor.visitQuerySpecification(StatementAnalyzer.java:1954)",  # noqa: E501
                    "io.trino.sql.analyzer.StatementAnalyzer$Visitor.visitQuerySpecification(StatementAnalyzer.java:361)",  # noqa: E501
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


@pytest.fixture
def mock_get_and_post():
    post = MagicMock()
    get = MagicMock()

    with patch("trino.client.TrinoRequest.http") as mock_requests:
        mock_requests.Session.return_value.get = get
        mock_requests.Session.return_value.post = post

        yield get, post


def sqlalchemy_version() -> str:
    import sqlalchemy
    return sqlalchemy.__version__
