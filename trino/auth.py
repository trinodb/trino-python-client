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
import json
import os
import re
import threading
from typing import Optional

from requests import Request
from requests.auth import AuthBase, extract_cookies_to_jar
from requests.utils import parse_dict_header

import trino.logging
from trino.client import exceptions

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

    def __eq__(self, other):
        if not isinstance(other, KerberosAuthentication):
            return False
        return (self._config == other._config
                and self._service_name == other._service_name
                and self._mutual_authentication == other._mutual_authentication
                and self._force_preemptive == other._force_preemptive
                and self._hostname_override == other._hostname_override
                and self._sanitize_mutual_error_response == other._sanitize_mutual_error_response
                and self._principal == other._principal
                and self._delegate == other._delegate
                and self._ca_bundle == other._ca_bundle)


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

    def __eq__(self, other):
        if not isinstance(other, BasicAuthentication):
            return False
        return self._username == other._username and self._password == other._password


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

    def __eq__(self, other):
        if not isinstance(other, JWTAuthentication):
            return False
        return self.token == other.token


def handle_redirect_auth_url(auth_url):
    print("Open the following URL in browser for the external authentication:")
    print(auth_url)


class _OAuth2TokenBearer(AuthBase):
    """
    Custom implementation of Trino Oauth2 based authorization to get the token
    """
    MAX_OAUTH_ATTEMPTS = 5
    _BEARER_PREFIX = re.compile(r"bearer", flags=re.IGNORECASE)

    def __init__(self, http_session, redirect_auth_url_handler=handle_redirect_auth_url):
        self._redirect_auth_url = redirect_auth_url_handler
        self._thread_local = threading.local()
        http_session.hooks['response'].append(self._authenticate)

    def __call__(self, r):
        if hasattr(self._thread_local, 'token') and self._thread_local.token:
            r.headers['Authorization'] = "Bearer " + self._thread_local.token

        r.register_hook('response', self._authenticate)

        return r

    def _authenticate(self, response, **kwargs):
        if not 400 <= response.status_code < 500:
            return response

        # we have to handle the authentication, may be token the token expired or it wasn't there at all
        auth_info = response.headers.get('WWW-Authenticate')
        if not auth_info:
            raise exceptions.TrinoAuthError("Error: header WWW-Authenticate not available in the response.")

        if not _OAuth2TokenBearer._BEARER_PREFIX.search(auth_info):
            raise exceptions.TrinoAuthError(f"Error: header info didn't match {auth_info}")

        auth_info_headers = parse_dict_header(_OAuth2TokenBearer._BEARER_PREFIX.sub("", auth_info, count=1))

        auth_server = auth_info_headers.get('x_redirect_server')
        if auth_server is None:
            raise exceptions.TrinoAuthError("Error: header info didn't have x_redirect_server")

        token_server = auth_info_headers.get('x_token_server')
        if token_server is None:
            raise exceptions.TrinoAuthError("Error: header info didn't have x_token_server")

        self._thread_local.token_server = token_server

        # tell app that use this url to proceed with the authentication
        self._redirect_auth_url(auth_server)

        # Consume content and release the original connection
        # to allow our new request to reuse the same one.
        response.content
        response.close()

        self._thread_local.token = self._get_token(token_server, response, **kwargs)
        return self._retry_request(response, **kwargs)

    def _retry_request(self, response, **kwargs):
        request = response.request.copy()
        extract_cookies_to_jar(request._cookies, response.request, response.raw)
        request.prepare_cookies(request._cookies)

        request.headers['Authorization'] = "Bearer " + self._thread_local.token
        retry_response = response.connection.send(request, **kwargs)
        retry_response.history.append(response)
        retry_response.request = request
        return retry_response

    def _get_token(self, token_server, response, **kwargs):
        attempts = 0
        while attempts < self.MAX_OAUTH_ATTEMPTS:
            attempts += 1
            with response.connection.send(Request(method='GET', url=token_server).prepare(), **kwargs) as response:
                if response.status_code == 200:
                    token_response = json.loads(response.text)
                    token = token_response.get('token')
                    if token:
                        return token
                    error = token_response.get('error')
                    if error:
                        raise exceptions.TrinoAuthError(f"Error while getting the token: {error}")
                    else:
                        token_server = token_response.get('nextUri')
                        logger.debug(f"nextURi auth token server: {token_server}")
                else:
                    raise exceptions.TrinoAuthError(
                        f"Error while getting the token response "
                        f"status code: {response.status_code}, "
                        f"body: {response.text}")

        raise exceptions.TrinoAuthError("Exceeded max attempts while getting the token")


class OAuth2Authentication(Authentication):
    def __init__(self, redirect_auth_url_handler=handle_redirect_auth_url):
        self._redirect_auth_url = redirect_auth_url_handler

    def set_http_session(self, http_session):
        http_session.auth = _OAuth2TokenBearer(http_session, self._redirect_auth_url)
        return http_session

    def get_exceptions(self):
        return ()

    def __eq__(self, other):
        if not isinstance(other, OAuth2Authentication):
            return False
        return self._redirect_auth_url == other._redirect_auth_url
