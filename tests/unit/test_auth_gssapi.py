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
from contextlib import nullcontext as does_not_raise
from typing import Any

import pytest
import requests

gssapi = pytest.importorskip("gssapi", exc_type=ImportError)

from trino.auth import GSSAPIAuthentication  # noqa: E402


class MockGssapiCredentials:
    def __init__(self, name: gssapi.Name, usage: str):
        self.name = name
        self.usage = usage

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MockGssapiCredentials):
            return False
        return (
            self.name == other.name,
            self.usage == other.usage,
        )


@pytest.fixture
def mock_gssapi_creds(monkeypatch):
    monkeypatch.setattr("gssapi.Credentials", MockGssapiCredentials)


def _gssapi_uname(spn: str):
    return gssapi.Name(spn, gssapi.NameType.user)


def _gssapi_sname(principal: str):
    return gssapi.Name(principal, gssapi.NameType.hostbased_service)


@pytest.mark.parametrize(
    "options, expected_credentials, expected_hostname, expected_exception",
    [
        (
            {}, None, None, does_not_raise(),
        ),
        (
            {"hostname_override": "foo"}, None, "foo", does_not_raise(),
        ),
        (
            {"service_name": "bar"}, None, None,
            pytest.raises(ValueError, match=r"must be used together with hostname_override"),
        ),
        (
            {"hostname_override": "foo", "service_name": "bar"}, None, _gssapi_sname("bar@foo"), does_not_raise(),
        ),
        (
            {"principal": "foo"}, MockGssapiCredentials(_gssapi_uname("foo"), "initial"), None, does_not_raise(),
        ),
    ]
)
def test_authentication_gssapi_init_arguments(
    options,
    expected_credentials,
    expected_hostname,
    expected_exception,
    mock_gssapi_creds,
    monkeypatch,
):
    auth = GSSAPIAuthentication(**options)

    session = requests.Session()

    with expected_exception:
        auth.set_http_session(session)

        assert session.auth.target_name == expected_hostname
        assert session.auth.creds == expected_credentials
