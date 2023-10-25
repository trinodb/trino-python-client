from typing import Any, Dict, List
from unittest import mock

import pytest
from sqlalchemy.engine.url import URL, make_url

from trino.auth import BasicAuthentication, OAuth2Authentication
from trino.dbapi import Connection
from trino.sqlalchemy import URL as trino_url
from trino.sqlalchemy.dialect import (
    CertificateAuthentication,
    JWTAuthentication,
    TrinoDialect,
)
from trino.transaction import IsolationLevel


class TestTrinoDialect:
    def setup_method(self):
        self.dialect = TrinoDialect()

    @pytest.mark.parametrize(
        "url, generated_url, expected_args, expected_kwargs",
        [
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                )),
                'trino://user@localhost:8080/?source=trino-sqlalchemy',
                list(),
                dict(host="localhost", catalog="system", user="user", port=8080, source="trino-sqlalchemy"),
            ),
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                    port=443,
                )),
                'trino://user@localhost:443/?source=trino-sqlalchemy',
                list(),
                dict(
                    host="localhost",
                    port=443,
                    catalog="system",
                    user="user",
                    source="trino-sqlalchemy",
                    http_scheme="https",
                ),
            ),
            (
                make_url(trino_url(
                    user="user",
                    password="pass",
                    host="localhost",
                    source="trino-rulez",
                )),
                'trino://user:***@localhost:8080/?source=trino-rulez',
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user",
                    auth=BasicAuthentication("user", "pass"),
                    http_scheme="https",
                    source="trino-rulez"
                ),
            ),
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                    cert="/my/path/to/cert",
                    key="afdlsdfk%4#'",
                )),
                'trino://user@localhost:8080/'
                '?cert=%2Fmy%2Fpath%2Fto%2Fcert'
                '&key=afdlsdfk%254%23%27'
                '&source=trino-sqlalchemy',
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user",
                    auth=CertificateAuthentication("/my/path/to/cert", "afdlsdfk%4#'"),
                    http_scheme="https",
                    source="trino-sqlalchemy"
                ),
            ),
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                    access_token="afdlsdfk%4#'",
                )),
                'trino://user@localhost:8080/'
                '?access_token=afdlsdfk%254%23%27'
                '&source=trino-sqlalchemy',
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user",
                    auth=JWTAuthentication("afdlsdfk%4#'"),
                    http_scheme="https",
                    source="trino-sqlalchemy"
                ),
            ),
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                    session_properties={"query_max_run_time": "1d"},
                    http_headers={"trino": 1},
                    extra_credential=[("a", "b"), ("c", "d")],
                    client_tags=["1", "sql"],
                    legacy_primitive_types=False,
                )),
                'trino://user@localhost:8080/'
                '?client_tags=%5B%221%22%2C+%22sql%22%5D'
                '&extra_credential=%5B%5B%22a%22%2C+%22b%22%5D%2C+%5B%22c%22%2C+%22d%22%5D%5D'
                '&http_headers=%7B%22trino%22%3A+1%7D'
                '&legacy_primitive_types=false'
                '&session_properties=%7B%22query_max_run_time%22%3A+%221d%22%7D'
                '&source=trino-sqlalchemy',
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user",
                    source="trino-sqlalchemy",
                    session_properties={"query_max_run_time": "1d"},
                    http_headers={"trino": 1},
                    extra_credential=[("a", "b"), ("c", "d")],
                    client_tags=["1", "sql"],
                    legacy_primitive_types=False,
                ),
            ),
            # url encoding
            (
                make_url(trino_url(
                    user="user@test.org/my_role",
                    password="pass /*&",
                    host="localhost",
                    session_properties={"query_max_run_time": "1d"},
                    http_headers={"trino": 1},
                    extra_credential=[
                        ("user1@test.org/my_role", "user2@test.org/my_role"),
                        ("user3@test.org/my_role", "user36@test.org/my_role")],
                    legacy_primitive_types=False,
                    client_tags=["1 @& /\"", "sql"],
                    verify=False,
                )),
                'trino://user%40test.org%2Fmy_role:***@localhost:8080/'
                '?client_tags=%5B%221+%40%26+%2F%5C%22%22%2C+%22sql%22%5D'
                '&extra_credential=%5B%5B%22user1%40test.org%2Fmy_role%22%2C+'
                '%22user2%40test.org%2Fmy_role%22%5D%2C+'
                '%5B%22user3%40test.org%2Fmy_role%22%2C+'
                '%22user36%40test.org%2Fmy_role%22%5D%5D'
                '&http_headers=%7B%22trino%22%3A+1%7D'
                '&legacy_primitive_types=false'
                '&session_properties=%7B%22query_max_run_time%22%3A+%221d%22%7D'
                '&source=trino-sqlalchemy'
                '&verify=false',
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user@test.org/my_role",
                    auth=BasicAuthentication("user@test.org/my_role", "pass /*&"),
                    http_scheme="https",
                    source="trino-sqlalchemy",
                    session_properties={"query_max_run_time": "1d"},
                    http_headers={"trino": 1},
                    extra_credential=[
                        ("user1@test.org/my_role", "user2@test.org/my_role"),
                        ("user3@test.org/my_role", "user36@test.org/my_role")],
                    legacy_primitive_types=False,
                    client_tags=["1 @& /\"", "sql"],
                    verify=False,
                ),
            ),
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                    roles={
                        "hive": "finance",
                        "system": "analyst",
                    }
                )),
                'trino://user@localhost:8080/'
                '?roles=%7B%22hive%22%3A+%22finance%22%2C+%22system%22%3A+%22analyst%22%7D&source=trino-sqlalchemy',
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user",
                    roles={"hive": "finance", "system": "analyst"},
                    source="trino-sqlalchemy",
                ),
            ),
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                    client_tags=["1", "sql"],
                    legacy_prepared_statements=False,
                )),
                'trino://user@localhost:8080/'
                '?client_tags=%5B%221%22%2C+%22sql%22%5D'
                '&legacy_prepared_statements=false'
                '&source=trino-sqlalchemy',
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user",
                    source="trino-sqlalchemy",
                    client_tags=["1", "sql"],
                    legacy_prepared_statements=False,
                ),
            ),
        ],
    )
    def test_create_connect_args(
        self,
        url: URL,
        generated_url: str,
        expected_args: List[Any],
        expected_kwargs: Dict[str, Any]
    ):
        assert repr(url) == generated_url

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


def test_trino_connection_basic_auth():
    dialect = TrinoDialect()
    username = 'trino-user'
    password = 'trino-bunny'
    url = make_url(f'trino://{username}:{password}@host')
    _, cparams = dialect.create_connect_args(url)

    assert cparams['http_scheme'] == "https"
    assert isinstance(cparams['auth'], BasicAuthentication)
    assert cparams['auth']._username == username
    assert cparams['auth']._password == password


def test_trino_connection_jwt_auth():
    dialect = TrinoDialect()
    access_token = 'sample-token'
    url = make_url(f'trino://host/?access_token={access_token}')
    _, cparams = dialect.create_connect_args(url)

    assert cparams['http_scheme'] == "https"
    assert isinstance(cparams['auth'], JWTAuthentication)
    assert cparams['auth'].token == access_token


def test_trino_connection_certificate_auth():
    dialect = TrinoDialect()
    cert = '/path/to/cert.pem'
    key = '/path/to/key.pem'
    url = make_url(f'trino://host/?cert={cert}&key={key}')
    _, cparams = dialect.create_connect_args(url)

    assert cparams['http_scheme'] == "https"
    assert isinstance(cparams['auth'], CertificateAuthentication)
    assert cparams['auth']._cert == cert
    assert cparams['auth']._key == key


def test_trino_connection_oauth2_auth():
    dialect = TrinoDialect()
    url = make_url('trino://host/?externalAuthentication=true')
    _, cparams = dialect.create_connect_args(url)

    assert cparams['http_scheme'] == "https"
    assert isinstance(cparams['auth'], OAuth2Authentication)
