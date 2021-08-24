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
import json
import time
import threading
import trino.logging

from enum import Enum
from typing import Optional
from requests.auth import AuthBase
from trino.client import exceptions, PROXIES

logger = trino.logging.get_logger(__name__)


class Authentication(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def set_http_session(self, http_session):
        pass

    def get_exceptions(self):
        return tuple()


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

    def set_http_session(self, http_session):
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
        return http_session

    def get_exceptions(self):
        try:
            from requests_kerberos.exceptions import KerberosExchangeError

            return (KerberosExchangeError,)
        except ImportError:
            raise RuntimeError("unable to import requests_kerberos")


class BasicAuthentication(Authentication):
    def __init__(self, username, password):
        self._username = username
        self._password = password

    def set_http_session(self, http_session):
        try:
            import requests.auth
        except ImportError:
            raise RuntimeError("unable to import requests.auth")

        http_session.auth = requests.auth.HTTPBasicAuth(self._username, self._password)
        return http_session

    def get_exceptions(self):
        return ()


class _BearerAuth(AuthBase):
    """
    Custom implementation of Authentication class for bearer token
    """
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = "Bearer " + self.token
        return r


class JWTAuthentication(Authentication):

    def __init__(self, token):
        self.token = token

    def set_http_session(self, http_session):
        http_session.auth = _BearerAuth(self.token)
        return http_session

    def get_exceptions(self):
        return ()


def handle_redirect_auth_url(auth_url):
    print("Open the following URL in browser for the external authentication:")
    print(auth_url)


class _OAuth2TokenBearer(AuthBase):
    """
    Custom implementation of Trino Oauth2 based authorization to get the token
    """
    MAX_OAUTH_ATTEMPTS = 5

    class _AuthStep(Enum):
        GET_REDIRECT_SERVER = 0
        GET_TOKEN = 1

    def __init__(self, http_session, redirect_auth_url_handler=handle_redirect_auth_url):
        self._redirect_auth_url = redirect_auth_url_handler
        self.http_session = http_session
        self._thread_local = threading.local()

    def __call__(self, r):
        if not hasattr(self._thread_local, 'init'):
            self._thread_local.init = True
            self._thread_local.token = None
            self._thread_local.token_server = None
            self._thread_local.attempts = 0
            self._thread_local.auth_step = self._AuthStep.GET_REDIRECT_SERVER

        if self._thread_local.token:
            r.headers['Authorization'] = "Bearer " + self._thread_local.token

        r.register_hook('response', self.__authenticate)

        return r

    def __authenticate(self, r, **kwargs):
        if (self._thread_local.auth_step == self._AuthStep.GET_REDIRECT_SERVER):
            self.__process_get_redirect_server(r)
        elif (self._thread_local.auth_step == self._AuthStep.GET_TOKEN):
            self.__process_get_token(r)
        return r

    def __process_get_redirect_server(self, r):
        if not 400 <= r.status_code < 500:
            return r

        # we have to handle the authentication, may be token the token expired or it wasn't there at all
        auth_info = r.headers.get('WWW-Authenticate')
        if not auth_info:
            raise exceptions.TrinoAuthError("Error: header WWW-Authenticate not available in the response.")

        matches = dict((s.strip().split('=', 1) for s in auth_info.split(",")))
        if matches is None:
            raise exceptions.TrinoAuthError(f"Error: header info didn't match {auth_info}")

        auth_server = matches.get('Bearer x_redirect_server')
        if auth_server is None:
            raise exceptions.TrinoAuthError("Error: header info didn't have Bearer x_redirect_server")

        token_server = matches.get('x_token_server')
        if token_server is None:
            raise exceptions.TrinoAuthError("Error: header info didn't have x_token_server")

        self._thread_local.token_server = token_server.strip('"')

        # tell app that use this url to proceed with the authentication
        self._redirect_auth_url(auth_server.strip('"'))

        # go to the next step, request a token from the token server
        self.__request_token()

        return r

    def __request_token(self):
        self._thread_local.auth_step = self._AuthStep.GET_TOKEN
        self._thread_local.attempts += 1

        if self._thread_local.attempts < self.MAX_OAUTH_ATTEMPTS:
            time.sleep(1)
            self.http_session.get(self._thread_local.token_server, proxies=PROXIES)
        else:
            raise exceptions.TrinoAuthError("Exceeded max attempts while getting the token")

    def __process_get_token(self, r):
        token = None
        if self._thread_local.attempts < self.MAX_OAUTH_ATTEMPTS:
            response = r
            if response.status_code == 200:
                token_response = json.loads(response.text)
                token = token_response.get('token')
                if token:
                    self._thread_local.token = token
                    self._thread_local.attempts = 0
                    self._thread_local.auth_step = self._AuthStep.GET_REDIRECT_SERVER
                    return r
                error = token_response.get('error')
                if error:
                    raise exceptions.TrinoAuthError(f"Error while getting the token : {error}")
                else:
                    token_server = token_response.get('nextUri')
                    logger.debug(f"nextURi auth token server {token_server}")
                    self._thread_local.token_server = token_server
                    self.__request_token()
            else:
                raise exceptions.TrinoAuthError(
                    f"Error while getting the token response status code:{response.status_code}")


class OAuth2Authentication(Authentication):
    def __init__(self, redirect_auth_url_handler=handle_redirect_auth_url):
        self._redirect_auth_url = redirect_auth_url_handler

    def set_http_session(self, http_session):
        http_session.auth = _OAuth2TokenBearer(http_session, self._redirect_auth_url)
        return http_session

    def get_exceptions(self):
        return ()
