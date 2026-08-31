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
import asyncio
import importlib
import json
import os
import re
import threading
import webbrowser
from collections.abc import AsyncGenerator
from collections.abc import Generator
from collections.abc import Mapping
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import httpx2

import trino.logging
from trino import exceptions
from trino._spnego import SPNEGOAuth
from trino.constants import HEADER_ORIGINAL_USER
from trino.constants import HEADER_USER
from trino.constants import MAX_NT_PASSWORD_SIZE

logger = trino.logging.get_logger(__name__)


class Authentication(metaclass=abc.ABCMeta):
    """
    Extension point for Trino authentication mechanisms.

    httpx builds its TLS and environment configuration when a client is
    constructed, so an authentication is asked for two things:

    - :meth:`get_client_arguments`: constructor arguments merged into the
      ``httpx2.Client``/``httpx2.AsyncClient`` the connection creates
      (``verify``, ``cert`` and ``trust_env``).
    - :meth:`get_http_auth`: the ``httpx2.Auth`` instance attached to the
      client, or ``None`` when the mechanism only needs client arguments.
    """

    def get_client_arguments(self) -> Dict[str, Any]:
        return {}

    @abc.abstractmethod
    def get_http_auth(self) -> Optional[httpx2.Auth]:
        pass

    def get_exceptions(self) -> Tuple[Any, ...]:
        return tuple()

    def set_http_session(self, http_session: Any) -> Any:
        raise NotImplementedError(
            "set_http_session was removed when the client migrated from requests to httpx2. "
            "Implement get_http_auth() (returning an httpx2.Auth) and, for verify/cert/trust_env, "
            "get_client_arguments() instead."
        )


def _gssapi_credentials(principal: Optional[str]) -> Any:
    if principal:
        try:
            import gssapi
        except ImportError:
            raise RuntimeError("unable to import gssapi")

        name = gssapi.Name(principal, gssapi.NameType.user)
        return gssapi.Credentials(name=name, usage="initiate")

    return None


class KerberosAuthentication(Authentication):
    MUTUAL_REQUIRED = 1
    MUTUAL_OPTIONAL = 2
    MUTUAL_DISABLED = 3

    def __init__(
        self,
        config: Optional[str] = None,
        service_name: Optional[str] = None,
        mutual_authentication: int = MUTUAL_REQUIRED,
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

    def get_client_arguments(self) -> Dict[str, Any]:
        if self._config:
            os.environ["KRB5_CONFIG"] = self._config
        arguments: Dict[str, Any] = {"trust_env": False}
        if self._ca_bundle:
            arguments["verify"] = self._ca_bundle
        return arguments

    def get_http_auth(self) -> Optional[httpx2.Auth]:
        return SPNEGOAuth(
            service_name=self._service_name,
            hostname_override=self._hostname_override,
            mutual_authentication=self._mutual_authentication,
            opportunistic_auth=self._force_preemptive,
            delegate=self._delegate,
            creds=_gssapi_credentials(self._principal),
            sanitize_mutual_error_response=self._sanitize_mutual_error_response,
        )

    def get_exceptions(self) -> Tuple[Any, ...]:
        return exceptions.SPNEGOExchangeError,

    def __eq__(self, other: object) -> bool:
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


class GSSAPIAuthentication(Authentication):
    MUTUAL_REQUIRED = 1
    MUTUAL_OPTIONAL = 2
    MUTUAL_DISABLED = 3

    def __init__(
        self,
        config: Optional[str] = None,
        service_name: Optional[str] = None,
        mutual_authentication: int = MUTUAL_DISABLED,
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

    def get_client_arguments(self) -> Dict[str, Any]:
        if self._config:
            os.environ["KRB5_CONFIG"] = self._config
        arguments: Dict[str, Any] = {"trust_env": False}
        if self._ca_bundle:
            arguments["verify"] = self._ca_bundle
        return arguments

    def get_http_auth(self) -> Optional[httpx2.Auth]:
        return SPNEGOAuth(
            target_name=self._get_target_name(self._hostname_override, self._service_name),
            mutual_authentication=self._mutual_authentication,
            opportunistic_auth=self._force_preemptive,
            delegate=self._delegate,
            creds=self._get_credentials(self._principal),
            sanitize_mutual_error_response=self._sanitize_mutual_error_response,
        )

    def _get_credentials(self, principal: Optional[str] = None) -> Any:
        return _gssapi_credentials(principal)

    def _get_target_name(
            self,
            hostname_override: Optional[str] = None,
            service_name: Optional[str] = None,
    ) -> Any:
        if service_name is not None:
            try:
                import gssapi
            except ImportError:
                raise RuntimeError("unable to import gssapi")

            if hostname_override is None:
                raise ValueError("service name must be used together with hostname_override")

            kerb_spn = "{0}@{1}".format(service_name, hostname_override)
            return gssapi.Name(kerb_spn, gssapi.NameType.hostbased_service)

        return hostname_override

    def get_exceptions(self) -> Tuple[Any, ...]:
        return exceptions.SPNEGOExchangeError,

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GSSAPIAuthentication):
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
    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password

    def get_http_auth(self) -> Optional[httpx2.Auth]:
        return httpx2.BasicAuth(self._username, self._password)

    def get_exceptions(self) -> Tuple[Any, ...]:
        return ()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BasicAuthentication):
            return False
        return self._username == other._username and self._password == other._password


class _BearerAuth(httpx2.Auth):
    """
    Custom implementation of Authentication class for bearer token
    """

    def __init__(self, token: str):
        self.token = token

    def auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response, None]:
        request.headers["Authorization"] = "Bearer " + self.token
        yield request


class JWTAuthentication(Authentication):

    def __init__(self, token: str):
        self.token = token

    def get_http_auth(self) -> Optional[httpx2.Auth]:
        return _BearerAuth(self.token)

    def get_exceptions(self) -> Tuple[Any, ...]:
        return ()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JWTAuthentication):
            return False
        return self.token == other.token


class RedirectHandler(metaclass=abc.ABCMeta):
    """
    Abstract class for OAuth redirect handlers, inherit from this class to implement your own redirect handler.
    """

    @abc.abstractmethod
    def __call__(self, url: str) -> None:
        raise NotImplementedError()


class ConsoleRedirectHandler(RedirectHandler):
    """
    Handler for OAuth redirections to log to console.
    """

    def __call__(self, url: str) -> None:
        print(f"Open the following URL in browser for the external authentication:\n{url}", flush=True)


class WebBrowserRedirectHandler(RedirectHandler):
    """
    Handler for OAuth redirections to open in web browser.
    """

    def __call__(self, url: str) -> None:
        webbrowser.open_new(url)


class CompositeRedirectHandler(RedirectHandler):
    """
    Composite handler for OAuth redirect handlers.
    """

    def __init__(self, handlers: List[Callable[[str], None]]):
        self.handlers = handlers

    def __call__(self, url: str) -> None:
        for handler in self.handlers:
            handler(url)


class _OAuth2TokenCache(metaclass=abc.ABCMeta):
    """
    Abstract class for OAuth token cache, inherit from this class to implement your own token cache.
    """

    @abc.abstractmethod
    def get_token_from_cache(self, key: Optional[str]) -> Optional[str]:
        pass

    @abc.abstractmethod
    def store_token_to_cache(self, key: Optional[str], token: str) -> None:
        pass


class _OAuth2TokenInMemoryCache(_OAuth2TokenCache):
    """
    Multiple clients can share the same cache only if each connection explicitly specifies
    a user otherwise the first cached token will be used to authenticate all other users.
    """

    def __init__(self) -> None:
        self._cache: Dict[Optional[str], str] = {}

    def get_token_from_cache(self, key: Optional[str]) -> Optional[str]:
        return self._cache.get(key)

    def store_token_to_cache(self, key: Optional[str], token: str) -> None:
        self._cache[key] = token


class _OAuth2KeyRingTokenCache(_OAuth2TokenCache):
    """
    Keyring token cache implementation
    """

    def __init__(self) -> None:
        super().__init__()
        try:
            self._keyring = importlib.import_module("keyring")
        except ImportError:
            self._keyring = None  # type: ignore
            logger.info("keyring module not found. OAuth2 token will not be stored in keyring.")

    def is_keyring_available(self) -> bool:
        return self._keyring is not None \
            and not isinstance(self._keyring.get_keyring(), self._keyring.backends.fail.Keyring)

    def get_token_from_cache(self, key: Optional[str]) -> Optional[str]:
        password = self._keyring.get_password(key, "token")

        try:
            password_as_dict = json.loads(str(password))
            if password_as_dict.get("sharded_password"):
                # if password was stored shared, reconstruct it
                shard_count = int(password_as_dict.get("shard_count"))

                password = ""
                for i in range(shard_count):
                    password += str(self._keyring.get_password(key, f"token__{i}"))

        except self._keyring.errors.NoKeyringError as e:
            raise trino.exceptions.NotSupportedError("Although keyring module is installed no backend has been "
                                                     "detected, check https://pypi.org/project/keyring/ for more "
                                                     "information.") from e
        except ValueError:
            pass

        return password

    def store_token_to_cache(self, key: Optional[str], token: str) -> None:
        # keyring is installed, so we can store the token for reuse within multiple threads
        try:
            # if not Windows or "small" password, stick to the default
            if os.name != "nt" or len(token) < MAX_NT_PASSWORD_SIZE:
                self._keyring.set_password(key, "token", token)
            else:
                logger.debug(f"password is {len(token)} characters, sharding it.")

                password_shards = [
                    token[i: i + MAX_NT_PASSWORD_SIZE] for i in range(0, len(token), MAX_NT_PASSWORD_SIZE)
                ]
                shard_info = {
                    "sharded_password": True,
                    "shard_count": len(password_shards),
                }

                # store the "shard info" as the "base" password
                self._keyring.set_password(key, "token", json.dumps(shard_info))
                # then store all shards with the shard number as postfix
                for i, s in enumerate(password_shards):
                    self._keyring.set_password(key, f"token__{i}", s)
        except self._keyring.errors.NoKeyringError as e:
            raise trino.exceptions.NotSupportedError("Although keyring module is installed no backend has been "
                                                     "detected, check https://pypi.org/project/keyring/ for more "
                                                     "information.") from e


# Sentinel yielded by the OAuth2 core flow instead of a request when another
# thread or task is already running the OAuth2 exchange; the drivers translate
# it into an appropriate (blocking or event-loop-friendly) wait.
_WAIT_FOR_OAUTH = object()


class _OAuth2TokenBearer(httpx2.Auth):
    """
    Custom implementation of Trino OAuth2 based authentication to get the token
    """
    MAX_OAUTH_ATTEMPTS = 5
    _BEARER_PREFIX = re.compile(r"bearer", flags=re.IGNORECASE)

    def __init__(self, redirect_auth_url_handler: Callable[[str], None]):
        self._redirect_auth_url = redirect_auth_url_handler
        keyring_cache = _OAuth2KeyRingTokenCache()
        self._token_cache: _OAuth2TokenCache = \
            keyring_cache if keyring_cache.is_keyring_available() else _OAuth2TokenInMemoryCache()
        self._token_lock = threading.Lock()
        # Held by the thread/task currently performing the OAuth2 exchange so
        # concurrent requests wait for its token instead of also opening a
        # browser window.
        self._inside_oauth_attempt_lock = threading.Lock()

    def sync_auth_flow(
        self, request: httpx2.Request
    ) -> Generator[httpx2.Request, httpx2.Response, None]:
        flow = self._flow(request)
        item = next(flow)
        while True:
            if item is _WAIT_FOR_OAUTH:
                # Block until the thread running the OAuth2 exchange finishes.
                with self._inside_oauth_attempt_lock:
                    pass
                item = flow.send(None)
                continue
            response = yield item
            response.read()
            try:
                item = flow.send(response)
            except StopIteration:
                return

    async def async_auth_flow(
        self, request: httpx2.Request
    ) -> AsyncGenerator[httpx2.Request, httpx2.Response]:
        flow = self._flow(request)
        item = next(flow)
        while True:
            if item is _WAIT_FOR_OAUTH:
                # Never block the event loop; the lock may be held by another
                # thread, so poll it instead of awaiting a loop-bound primitive.
                while self._inside_oauth_attempt_lock.locked():
                    await asyncio.sleep(0.05)
                item = flow.send(None)
                continue
            response = yield item
            await response.aread()
            try:
                item = flow.send(response)
            except StopIteration:
                return

    def _flow(self, request: httpx2.Request) -> Generator[Any, Any, None]:
        host = request.url.host
        user = self._determine_user(request.headers)
        key = self._construct_cache_key(host, user)
        token = self._get_token_from_cache(key)

        if token is not None:
            request.headers["Authorization"] = "Bearer " + token

        response = yield request
        if not 400 <= response.status_code < 500:
            return

        if self._inside_oauth_attempt_lock.acquire(blocking=False):
            try:
                # Lock is acquired, attempt the OAuth2 flow
                token = yield from self._attempt_oauth(response)
                self._store_token_to_cache(key, token)
            finally:
                self._inside_oauth_attempt_lock.release()
        else:
            # We are already in the OAuth2 flow on another thread or task;
            # wait until it finishes and pick up the token it cached.
            yield _WAIT_FOR_OAUTH
            token = self._get_token_from_cache(key)

        # Retry the original request with the fresh token, carrying over any
        # cookies the failed response may have set.
        if token is not None:
            request.headers["Authorization"] = "Bearer " + token
        cookies = httpx2.Cookies()
        cookies.extract_cookies(response)
        cookies.set_cookie_header(request)
        yield request

    def _attempt_oauth(self, response: httpx2.Response) -> Generator[Any, Any, str]:
        # we have to handle the authentication, may be token the token expired, or it wasn't there at all
        auth_info = response.headers.get('WWW-Authenticate')
        if not auth_info:
            raise exceptions.TrinoAuthError("Error: header WWW-Authenticate not available in the response.")

        if not _OAuth2TokenBearer._BEARER_PREFIX.search(auth_info):
            raise exceptions.TrinoAuthError(f"Error: header info didn't match {auth_info}")

        # Example www-authenticate header value:
        # 'Basic realm="Trino", Bearer realm="Trino", token_type="JWT",
        # Bearer x_redirect_server="https://trino.com/oauth2/token/uuid4",
        # x_token_server="https://trino.com/oauth2/token/uuid4"'
        auth_info_headers = self._parse_authenticate_header(auth_info)

        auth_server = auth_info_headers.get('bearer x_redirect_server', auth_info_headers.get('x_redirect_server'))
        token_server = auth_info_headers.get('bearer x_token_server', auth_info_headers.get('x_token_server'))
        if token_server is None:
            raise exceptions.TrinoAuthError("Error: header info didn't have x_token_server")

        if auth_server is not None:
            # tell app that use this url to proceed with the authentication
            self._redirect_auth_url(auth_server)

        # Token polls reuse the timeout the original request was sent with.
        extensions = {}
        timeout = response.request.extensions.get("timeout")
        if timeout is not None:
            extensions["timeout"] = timeout

        attempts = 0
        while attempts < self.MAX_OAUTH_ATTEMPTS:
            attempts += 1
            token_response = yield httpx2.Request("GET", token_server, extensions=extensions)
            if token_response.status_code == 200:
                body = json.loads(token_response.text)
                token = body.get('token')
                if token:
                    return token
                error = body.get('error')
                if error:
                    raise exceptions.TrinoAuthError(f"Error while getting the token: {error}")
                else:
                    token_server = body.get('nextUri')
                    logger.debug(f"nextURi auth token server: {token_server}")
            else:
                raise exceptions.TrinoAuthError(
                    f"Error while getting the token response "
                    f"status code: {token_response.status_code}, "
                    f"body: {token_response.text}")

        raise exceptions.TrinoAuthError("Exceeded max attempts while getting the token")

    def _get_token_from_cache(self, key: Optional[str]) -> Optional[str]:
        with self._token_lock:
            return self._token_cache.get_token_from_cache(key)

    def _store_token_to_cache(self, key: Optional[str], token: str) -> None:
        with self._token_lock:
            self._token_cache.store_token_to_cache(key, token)

    @staticmethod
    def _determine_user(headers: Mapping[Any, Any]) -> Optional[Any]:
        return headers.get(HEADER_ORIGINAL_USER, headers.get(HEADER_USER))

    @staticmethod
    def _construct_cache_key(host: Optional[str], user: Optional[str]) -> Optional[str]:
        if user is None:
            return host
        else:
            return f"{host}@{user}"

    @staticmethod
    def _parse_authenticate_header(header: str) -> Dict[str, str]:
        logger.debug(f"Authentication header: {header}")
        components = header.split(",")
        auth_info_headers = {}

        for component in components:
            component = component.strip()
            if "=" in component:
                key, value = component.split("=", 1)
                if value[0] == '"' and value[-1] == '"':
                    value = value[1:-1]
                auth_info_headers[key.lower()] = value
        return auth_info_headers


class OAuth2Authentication(Authentication):
    def __init__(self, redirect_auth_url_handler: CompositeRedirectHandler = CompositeRedirectHandler([
        WebBrowserRedirectHandler(),
        ConsoleRedirectHandler()
    ])):
        self._redirect_auth_url = redirect_auth_url_handler
        self._bearer = _OAuth2TokenBearer(self._redirect_auth_url)

    def get_http_auth(self) -> Optional[httpx2.Auth]:
        return self._bearer

    def get_exceptions(self) -> Tuple[Any, ...]:
        return ()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OAuth2Authentication):
            return False
        return self._redirect_auth_url == other._redirect_auth_url


class CertificateAuthentication(Authentication):
    def __init__(self, cert: str, key: str):
        self._cert = cert
        self._key = key

    def get_client_arguments(self) -> Dict[str, Any]:
        return {"cert": (self._cert, self._key)}

    def get_http_auth(self) -> Optional[httpx2.Auth]:
        return None

    def get_exceptions(self) -> Tuple[Any, ...]:
        return ()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CertificateAuthentication):
            return False
        return self._cert == other._cert and self._key == other._key
