import threading
import time
from typing import Any
from typing import Dict
from typing import List
from unittest import mock

import pytest
from sqlalchemy import exc
from sqlalchemy.engine.url import make_url
from sqlalchemy.engine.url import URL

import trino.exceptions
from trino.auth import BasicAuthentication
from trino.auth import OAuth2Authentication
from trino.dbapi import Connection
from trino.sqlalchemy import URL as trino_url
from trino.sqlalchemy.dialect import CertificateAuthentication
from trino.sqlalchemy.dialect import JWTAuthentication
from trino.sqlalchemy.dialect import TrinoDialect
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
                dict(host="localhost", port=443, catalog="system", user="user", source="trino-sqlalchemy"),
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
            (
                make_url(trino_url(
                    user="user",
                    password="pass",
                    host="localhost",
                    allow_insecure_auth=True,
                )),
                'trino://user:***@localhost:8080/'
                '?allow_insecure_auth=true'
                '&source=trino-sqlalchemy',
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user",
                    auth=BasicAuthentication("user", "pass"),
                    source="trino-sqlalchemy",
                    allow_insecure_auth=True,
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

    def test_create_connect_args_allow_insecure_auth_permits_connection_over_http(self):
        # Default port (8080) resolves to http_scheme="http" in Connection; without
        # allow_insecure_auth this would raise TrinoAuthError.
        url = make_url(trino_url(user="user", password="pass", host="localhost", allow_insecure_auth=True))
        args, kwargs = self.dialect.create_connect_args(url)
        assert kwargs["allow_insecure_auth"] is True

        Connection(*args, **kwargs)

    def test_create_connect_args_allow_insecure_auth_false_raises_over_http(self):
        url = make_url(trino_url(user="user", password="pass", host="localhost", allow_insecure_auth=False))
        args, kwargs = self.dialect.create_connect_args(url)
        assert kwargs["allow_insecure_auth"] is False

        with pytest.raises(trino.exceptions.TrinoAuthError, match="TLS/SSL is required for authentication"):
            Connection(*args, **kwargs)

    def test_create_connect_args_without_allow_insecure_auth_raises_over_http(self):
        url = make_url(trino_url(user="user", password="pass", host="localhost"))
        args, kwargs = self.dialect.create_connect_args(url)
        assert "allow_insecure_auth" not in kwargs

        with pytest.raises(trino.exceptions.TrinoAuthError, match="TLS/SSL is required for authentication"):
            Connection(*args, **kwargs)

    def test_get_default_isolation_level(self):
        isolation_level = self.dialect.get_default_isolation_level(mock.Mock())
        assert isolation_level == "AUTOCOMMIT"

    def test_isolation_level(self):
        dbapi_conn = Connection(host="localhost")

        self.dialect.set_isolation_level(dbapi_conn, "SERIALIZABLE")
        assert dbapi_conn._isolation_level == IsolationLevel.SERIALIZABLE

        isolation_level = self.dialect.get_isolation_level(dbapi_conn)
        assert isolation_level == "SERIALIZABLE"

    class _FakeCursor:
        def __init__(self, description):
            self.description = description

    class _FakeResult:
        """Hand-written fake standing in for a SQLAlchemy CursorResult."""

        def __init__(self, partition_names, data_types):
            self.cursor = TestTrinoDialect._FakeCursor(list(zip(partition_names, data_types)))
            self.close_calls = 0

        def close(self):
            self.close_calls += 1

    @staticmethod
    def _partitions_connection(partition_names, data_types):
        result = TestTrinoDialect._FakeResult(partition_names, data_types)
        connection = mock.Mock()
        connection.execute.return_value = result
        return connection, result

    def test_get_partitions_hive_table_returns_partition_names(self):
        partition_names = ["year", "month"]
        data_types = ["integer", "integer"]
        connection, result = self._partitions_connection(partition_names, data_types)

        returned = self.dialect._get_partitions(connection, "some_table", "some_schema")

        assert returned == partition_names
        assert result.close_calls == 1

    def test_get_partitions_iceberg_table_returns_none(self):
        # This is the exact shape of an Iceberg $partitions table.
        partition_names = ["partition", "record_count", "file_count", "total_size", "data"]
        data_types = ["row(...)", "bigint", "bigint", "bigint", "row(...)"]
        connection, result = self._partitions_connection(partition_names, data_types)

        returned = self.dialect._get_partitions(connection, "some_table", "some_schema")

        assert returned is None
        assert result.close_calls == 1

    def test_get_partitions_closes_result_even_on_error(self):
        connection = mock.Mock()
        result = mock.Mock()
        result.cursor.description = None  # triggers a TypeError while iterating
        connection.execute.return_value = result

        with pytest.raises(TypeError):
            self.dialect._get_partitions(connection, "some_table", "some_schema")

        result.close.assert_called_once()

    def test_get_partitions_defaults_schema_when_not_given(self):
        partition_names = ["year"]
        data_types = ["integer"]
        connection, _ = self._partitions_connection(partition_names, data_types)

        with mock.patch.object(
            self.dialect, "_get_default_schema_name", return_value="default_schema"
        ) as get_default_schema:
            self.dialect._get_partitions(connection, "some_table")

        get_default_schema.assert_called_once_with(connection)
        # The defaulted schema must appear in the query.
        query = str(connection.execute.call_args[0][0])
        assert "default_schema" in query


def test_trino_connection_basic_auth():
    dialect = TrinoDialect()
    username = 'trino-user'
    password = 'trino-bunny'
    url = make_url(f'trino://{username}:{password}@host')
    _, cparams = dialect.create_connect_args(url)

    assert isinstance(cparams['auth'], BasicAuthentication)
    assert cparams['auth']._username == username
    assert cparams['auth']._password == password


@pytest.mark.parametrize(
    "userinfo, expected_user, expected_password",
    [
        # A literal '+' in the userinfo must stay a '+'. The userinfo is not
        # form-encoded, so '+' does not mean space there.
        ("user+name:pass+word", "user+name", "pass+word"),
        # '%2B' is the percent-encoding of '+'. SQLAlchemy decodes it to '+', and
        # the dialect must not decode it a second time into a space.
        ("user%2Bname:pass%2Bword", "user+name", "pass+word"),
        # A percent-encoded space really is a space.
        ("user%20name:pass%20word", "user name", "pass word"),
    ],
)
def test_trino_connection_basic_auth_with_plus_in_credentials(userinfo, expected_user, expected_password):
    # SQLAlchemy's make_url already decodes the userinfo, so the dialect must not run
    # it through unquote_plus again (that would turn a literal '+' into a space).
    dialect = TrinoDialect()
    url = make_url(f"trino://{userinfo}@host")
    _, cparams = dialect.create_connect_args(url)

    assert cparams['user'] == expected_user
    assert isinstance(cparams['auth'], BasicAuthentication)
    assert cparams['auth']._username == expected_user
    assert cparams['auth']._password == expected_password


def test_trino_connection_query_params_preserve_plus():
    # Regression for the same double-decode on the other unquote_plus call sites.
    # SQLAlchemy already decodes the query values, so a '+' round-tripped through the
    # trino URL builder must survive. The catalog and schema are the exception: they
    # come from url.database, which SQLAlchemy leaves url-encoded, so they are still
    # decoded in the dialect.
    dialect = TrinoDialect()
    url = make_url(trino_url(
        host="host",
        user="us+er",
        password="pa+ss",
        catalog="ca+t",
        schema="sc+h",
        source="so+urce",
        session_properties={"prop": "1+1"},
        http_headers={"x-trino": "a+b"},
        extra_credential=[("cred", "va+lue")],
        client_tags=["ta+g"],
        roles={"system": "ro+le"},
    ))
    _, cparams = dialect.create_connect_args(url)

    assert cparams['user'] == 'us+er'
    assert cparams['auth']._password == 'pa+ss'
    assert cparams['catalog'] == 'ca+t'
    assert cparams['schema'] == 'sc+h'
    assert cparams['source'] == 'so+urce'
    assert cparams['session_properties'] == {"prop": "1+1"}
    assert cparams['http_headers'] == {"x-trino": "a+b"}
    assert cparams['extra_credential'] == [("cred", "va+lue")]
    assert cparams['client_tags'] == ["ta+g"]
    assert cparams['roles'] == {"system": "ro+le"}


def test_trino_connection_jwt_auth_preserves_plus():
    # '%2B' in the token is decoded to '+' by SQLAlchemy and must not be decoded again.
    dialect = TrinoDialect()
    url = make_url("trino://host/?access_token=tok%2Ben")
    _, cparams = dialect.create_connect_args(url)

    assert isinstance(cparams['auth'], JWTAuthentication)
    assert cparams['auth'].token == 'tok+en'


def test_trino_connection_certificate_auth_preserves_plus():
    dialect = TrinoDialect()
    url = make_url("trino://host/?cert=ce%2Brt&key=ke%2By")
    _, cparams = dialect.create_connect_args(url)

    assert isinstance(cparams['auth'], CertificateAuthentication)
    assert cparams['auth']._cert == 'ce+rt'
    assert cparams['auth']._key == 'ke+y'


def test_trino_connection_jwt_auth():
    dialect = TrinoDialect()
    access_token = 'sample-token'
    url = make_url(f'trino://host/?access_token={access_token}')
    _, cparams = dialect.create_connect_args(url)

    assert isinstance(cparams['auth'], JWTAuthentication)
    assert cparams['auth'].token == access_token


def test_trino_connection_certificate_auth():
    dialect = TrinoDialect()
    cert = '/path/to/cert.pem'
    key = '/path/to/key.pem'
    url = make_url(f'trino://host/?cert={cert}&key={key}')
    _, cparams = dialect.create_connect_args(url)

    assert isinstance(cparams['auth'], CertificateAuthentication)
    assert cparams['auth']._cert == cert
    assert cparams['auth']._key == key


def test_trino_connection_certificate_auth_cert_and_key_required():
    dialect = TrinoDialect()
    cert = '/path/to/cert.pem'
    key = '/path/to/key.pem'
    url = make_url(f'trino://host/?cert={cert}')
    _, cparams = dialect.create_connect_args(url)

    assert 'auth' not in cparams

    url = make_url(f'trino://host/?key={key}')
    _, cparams = dialect.create_connect_args(url)

    assert 'auth' not in cparams


def test_trino_connection_oauth2_auth():
    dialect = TrinoDialect()
    url = make_url('trino://host/?externalAuthentication=true')
    _, cparams = dialect.create_connect_args(url)

    assert isinstance(cparams['auth'], OAuth2Authentication)


@pytest.mark.parametrize(
    "host",
    [
        "https://trino.example.com",  # scheme prefix (the common mistake)
        "http://trino.example.com",
        "trino.example.com:443",      # port embedded in host
        "trino.example.com/path",     # path embedded in host
        "user@trino.example.com",     # credentials embedded in host
        "::1",                        # unbracketed IPv6 literal
        "[::1",                       # unbalanced bracket
        "example.com]",               # stray bracket
        "https://[::1",               # scheme plus malformed IPv6 literal
    ],
)
def test_url_rejects_non_hostname_host(host):
    with pytest.raises(exc.ArgumentError, match="host must be a hostname only"):
        trino_url(host=host, port=443, user="user")


@pytest.mark.parametrize(
    "host",
    [
        "trino.example.com",
        "localhost",
        "192.168.0.1",
        "[::1]",              # bracketed IPv6 literal
        "host_with.dots-and_underscores",
    ],
)
def test_url_accepts_bare_hostname(host):
    # A valid host must round-trip through make_url without error.
    assert make_url(trino_url(host=host, port=443, user="user")).host is not None


def test_server_version_info_does_not_recurse_and_is_cached():
    # Regression test for https://github.com/trinodb/trino-python-client/issues/559
    #
    # aws-xray-sdk reads `dialect.server_version_info` before every execute. Since
    # computing that property itself issues a `SELECT version()` query through
    # `connection.execute`, a naive implementation causes aws-xray to read the
    # property again while the query is in flight, recursing forever.
    dialect = TrinoDialect()

    class FakeResult:
        def __init__(self, value):
            self._value = value

        def scalar(self):
            return self._value

    class FakeConnection:
        def __init__(self, dialect):
            self.dialect = dialect
            self.execute_count = 0

        def execute(self, *args, **kwargs):
            # Mimic aws-xray-sdk's re-entrant read of server_version_info before
            # every execute call.
            self.dialect.server_version_info
            self.execute_count += 1
            return FakeResult("455")

    fake_conn = FakeConnection(dialect)

    dialect._get_server_version_info(fake_conn)

    version_info = dialect.server_version_info
    assert version_info == ("455",)
    assert fake_conn.execute_count == 1

    # Subsequent reads should be served from the cache and must not trigger
    # another query.
    assert dialect.server_version_info == ("455",)
    assert dialect.server_version_info == ("455",)
    assert fake_conn.execute_count == 1


def test_server_version_info_is_resolved_for_concurrent_readers():
    # The re-entrancy guard must be scoped to the thread doing the lookup. A reader on another
    # thread has to wait for the real version instead of observing the in-flight placeholder.
    dialect = TrinoDialect()
    query_in_flight = threading.Event()
    release_query = threading.Event()

    class FakeResult:
        def __init__(self, value):
            self._value = value

        def scalar(self):
            return self._value

    class FakeConnection:
        def __init__(self):
            self.execute_count = 0

        def execute(self, *args, **kwargs):
            self.execute_count += 1
            query_in_flight.set()
            release_query.wait(timeout=10)
            return FakeResult("455")

    fake_conn = FakeConnection()
    dialect._get_server_version_info(fake_conn)

    results = {}

    def read(name):
        results[name] = dialect.server_version_info

    first = threading.Thread(target=read, args=("first",))
    second = threading.Thread(target=read, args=("second",))

    first.start()
    assert query_in_flight.wait(timeout=10)
    second.start()
    # Give the second thread a chance to reach the getter while the query is still in flight.
    time.sleep(0.1)
    release_query.set()

    first.join(timeout=10)
    second.join(timeout=10)

    assert results == {"first": ("455",), "second": ("455",)}
    assert fake_conn.execute_count == 1


def test_server_version_info_handles_orig_without_message_attribute():
    # Only TrinoQueryError exposes `.message`. The DBAPI error wrapped by SQLAlchemy could be
    # any exception. Failing to read the version must not raise AttributeError from the log call.
    dialect = TrinoDialect()

    class FailingConnection:
        def execute(self, *args, **kwargs):
            raise exc.ProgrammingError("SELECT version()", None, Exception("boom"))

    dialect._get_server_version_info(FailingConnection())

    assert dialect.server_version_info is None
