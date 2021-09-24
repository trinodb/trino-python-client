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

import abc
import os
from typing import Optional, Sequence, Type

import requests
import requests.auth


class Authentication(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def set_http_session(self, http_session: requests.Session) -> None:
        pass

    def get_exceptions(self) -> Sequence[Type[Exception]]:
        return ()


class KerberosAuthentication(Authentication):
    def __init__(
        self,
        config: Optional[str] = None,
        service_name: str = None,
        mutual_authentication: bool = False,
        force_preemptive: bool = False,
        hostname_override: Optional[str] = None,
        sanitize_mutual_error_response: bool = True,
        principal: Optional[str] = None,
        delegate: bool = False,
        ca_bundle: Optional[str] = None,
    ) -> None:
        self._config = config
        self._service_name = service_name
        self._mutual_authentication = mutual_authentication
        self._force_preemptive = force_preemptive
        self._hostname_override = hostname_override
        self._sanitize_mutual_error_response = sanitize_mutual_error_response
        self._principal = principal
        self._delegate = delegate
        self._ca_bundle = ca_bundle

    def set_http_session(self, http_session: requests.Session) -> None:
        try:
            import requests_kerberos
        except ImportError:
            raise RuntimeError("unable to import requests_kerberos")

        if self._config:
            os.environ["KRB5_CONFIG"] = self._config
        http_session.trust_env = False
        http_session.auth = requests_kerberos.HTTPKerberosAuth(
            mutual_authentication=self._mutual_authentication,
            force_preemptive=self._force_preemptive,
            hostname_override=self._hostname_override,
            sanitize_mutual_error_response=self._sanitize_mutual_error_response,
            principal=self._principal,
            delegate=self._delegate,
            service=self._service_name,
        )
        if self._ca_bundle:
            http_session.verify = self._ca_bundle

    def get_exceptions(self) -> Sequence[Type[Exception]]:
        try:
            from requests_kerberos.exceptions import KerberosExchangeError

            return (KerberosExchangeError,)
        except ImportError:
            raise RuntimeError("unable to import requests_kerberos")


class BasicAuthentication(Authentication):
    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password

    def set_http_session(self, http_session: requests.Session) -> None:
        http_session.auth = requests.auth.HTTPBasicAuth(self._username, self._password)


class _BearerAuth(requests.auth.AuthBase):
    """
    Custom implementation of AuthBase class for bearer token
    """

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = "Bearer " + self.token
        return r


class JWTAuthentication(Authentication):
    def __init__(self, token: str) -> None:
        self.token = token

    def set_http_session(self, http_session: requests.Session) -> None:
        http_session.auth = _BearerAuth(self.token)
