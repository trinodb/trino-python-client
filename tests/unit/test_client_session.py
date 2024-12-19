from typing import Dict
from typing import Optional
from typing import Union

import pytest

from trino.client import ClientSession


def test_client_session_user() -> None:
    session = ClientSession(user="user")
    assert session.user == "user"


def test_client_session_authorization_user() -> None:
    session = ClientSession(user="user")
    assert session.authorization_user is None

    session.authorization_user = "fake"
    assert session.authorization_user == "fake"


def test_client_session_catalog() -> None:
    session = ClientSession(user="user")
    assert session.catalog is None

    session.catalog = "fake"
    assert session.catalog == "fake"


def test_client_session_schema() -> None:
    session = ClientSession(user="user")
    assert session.schema is None

    session.schema = "fake"
    assert session.schema == "fake"


def test_client_session_source() -> None:
    session = ClientSession(user="user")
    assert session.source is None


def test_client_session_properties() -> None:
    session = ClientSession(user="user")
    assert session.properties == {}

    session.properties = {"fake_k": "fake_v"}
    assert session.properties == {"fake_k": "fake_v"}


def test_client_session_headers() -> None:
    session = ClientSession(user="user")
    assert session.headers == {}


def test_client_session_transaction_id() -> None:
    session = ClientSession(user="user")
    assert session.transaction_id is None

    session.transaction_id = "fake"
    assert session.transaction_id == "fake"


def test_client_session_extra_credential() -> None:
    session = ClientSession(user="user")
    assert session.extra_credential is None


def test_client_session_extra_client_tags() -> None:
    session = ClientSession(user="user")
    assert session.client_tags == []


@pytest.mark.parametrize(
    argnames=["argument", "result"],
    argvalues=[
        (None, {}),
        ("fake", {"system": "ROLE{fake}"}),
        ("NONE", {"system": "NONE"}),
        ("ALL", {"system": "ALL"}),
        ({"hive": "fake"}, {"hive": "ROLE{fake}"}),
    ]
)
def test_client_session_roles(argument: Optional[Union[str, Dict[str, str]]], result: Dict[str, str]) -> None:
    session = ClientSession(user="user", roles=argument)
    assert session.roles == result


def test_client_session_timezone() -> None:
    session = ClientSession(user="user", timezone="UTC")
    assert session.timezone == "UTC"


def test_client_session_prepared_statements() -> None:
    session = ClientSession(user="user")
    assert session.prepared_statements == {}

    session.prepared_statements = {"fake_k": "fake_v"}
    assert session.prepared_statements == {"fake_k": "fake_v"}
