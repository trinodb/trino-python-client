from typing import Any, Dict, List
from unittest import mock

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.engine.url import URL

from trino.auth import BasicAuthentication
from trino.dbapi import Connection
from trino.sqlalchemy.dialect import TrinoDialect
from trino.transaction import IsolationLevel


class TestTrinoDialect:
    def setup(self):
        self.dialect = TrinoDialect()

    # TODO: Test more authentication methods and URL params (https://github.com/trinodb/trino-python-client/issues/106)
    @pytest.mark.parametrize(
        "url, expected_args, expected_kwargs",
        [
            (
                make_url("trino://user@localhost"),
                list(),
                dict(host="localhost", catalog="system", user="user"),
            ),
            (
                make_url("trino://user@localhost:8080"),
                list(),
                dict(host="localhost", port=8080, catalog="system", user="user"),
            ),
            (
                make_url("trino://user:pass@localhost:8080"),
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user",
                    auth=BasicAuthentication("user", "pass"),
                    http_scheme="https",
                ),
            ),
        ],
    )
    def test_create_connect_args(self, url: URL, expected_args: List[Any], expected_kwargs: Dict[str, Any]):
        actual_args, actual_kwargs = self.dialect.create_connect_args(url)

        assert actual_args == expected_args
        assert actual_kwargs == expected_kwargs

    def test_create_connect_args_missing_user_when_specify_password(self):
        url = make_url("trino://:pass@localhost")
        with pytest.raises(ValueError, match="Username is required when specify password in connection URL"):
            self.dialect.create_connect_args(url)

    def test_create_connect_args_wrong_db_format(self):
        url = make_url("trino://abc@localhost/catalog/schema/foobar")
        with pytest.raises(ValueError, match="Unexpected database format catalog/schema/foobar"):
            self.dialect.create_connect_args(url)

    def test_get_default_isolation_level(self):
        isolation_level = self.dialect.get_default_isolation_level(mock.Mock())
        assert isolation_level == "AUTOCOMMIT"

    def test_isolation_level(self):
        dbapi_conn = Connection(host="localhost")

        self.dialect.set_isolation_level(dbapi_conn, "SERIALIZABLE")
        assert dbapi_conn._isolation_level == IsolationLevel.SERIALIZABLE

        isolation_level = self.dialect.get_isolation_level(dbapi_conn)
        assert isolation_level == "SERIALIZABLE"
