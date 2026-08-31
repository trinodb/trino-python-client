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
"""
SPNEGO (Kerberos/GSSAPI "Negotiate") authentication flow for httpx2.

This is an in-repo port of the flow implemented by requests_kerberos /
requests_gssapi / httpx-gssapi, built directly on python-gssapi and the
``httpx2.Auth`` generator protocol so it works with both the synchronous and
asynchronous clients. It backs both ``trino.auth.KerberosAuthentication`` and
``trino.auth.GSSAPIAuthentication``.

Requires the ``gssapi`` package (the ``trino[gssapi]``/``trino[kerberos]``
extras) and a GSSAPI implementation such as MIT Kerberos. Unlike
requests_kerberos, Windows SSPI is not supported.
"""
from __future__ import annotations

import base64
import re
import typing

import httpx2

from trino.exceptions import SPNEGOExchangeError

# Mutual-authentication modes; the values match requests_kerberos/requests_gssapi.
MUTUAL_REQUIRED = 1
MUTUAL_OPTIONAL = 2
MUTUAL_DISABLED = 3

_NEGOTIATE_TOKEN_PATTERN = re.compile(r"Negotiate\s*([^,\s]*)", flags=re.IGNORECASE)

# The maximum number of 401 challenges answered before giving up, matching
# the requests_gssapi/httpx-gssapi behavior.
_MAX_CHALLENGES = 2


def _import_gssapi() -> typing.Any:
    try:
        import gssapi
    except ImportError:
        raise RuntimeError("unable to import gssapi")
    return gssapi


class SPNEGOAuth(httpx2.Auth):
    """
    httpx2 authentication flow answering HTTP Negotiate (SPNEGO) challenges.

    :param target_name: the GSSAPI target. Either a ``gssapi.Name``, a host
        name string (authenticates the default ``HTTP`` service at that host)
        or ``None`` (authenticates ``service_name`` at the request's host).
    :param service_name: Kerberos service, defaults to ``HTTP``.
    :param hostname_override: authenticate against this host instead of the
        host of the request URL.
    :param mutual_authentication: MUTUAL_REQUIRED, MUTUAL_OPTIONAL or
        MUTUAL_DISABLED. When enabled, the server's final Negotiate token is
        verified; on MUTUAL_REQUIRED a missing or invalid token raises
        :class:`~trino.exceptions.SPNEGOExchangeError`.
    :param opportunistic_auth: send an initial token preemptively instead of
        waiting for a 401 challenge.
    :param delegate: request credential delegation.
    :param creds: explicit ``gssapi.Credentials`` to use.
    :param sanitize_mutual_error_response: accepted for signature compatibility
        with requests_kerberos/requests_gssapi; response bodies of failed
        exchanges are never exposed as authenticated content by this client.
    """

    def __init__(
        self,
        *,
        target_name: typing.Any = None,
        service_name: typing.Optional[str] = None,
        hostname_override: typing.Optional[str] = None,
        mutual_authentication: int = MUTUAL_DISABLED,
        opportunistic_auth: bool = False,
        delegate: bool = False,
        creds: typing.Any = None,
        sanitize_mutual_error_response: bool = True,
    ) -> None:
        # Fail early when gssapi is missing, like the requests-era classes did.
        _import_gssapi()
        self.target_name = target_name
        self.service_name = service_name
        self.hostname_override = hostname_override
        self.mutual_authentication = mutual_authentication
        self.opportunistic_auth = opportunistic_auth
        self.delegate = delegate
        self.creds = creds
        self.sanitize_mutual_error_response = sanitize_mutual_error_response

    def _resolve_target_name(self, request_host: str) -> typing.Any:
        gssapi = _import_gssapi()
        if isinstance(self.target_name, gssapi.Name):
            return self.target_name
        # A plain-string target is a host name carrying the default service.
        host = self.target_name or self.hostname_override or request_host
        service = self.service_name or "HTTP"
        return gssapi.Name(f"{service}@{host}", gssapi.NameType.hostbased_service)

    def _make_context(self, request_host: str) -> typing.Any:
        gssapi = _import_gssapi()
        flags = [gssapi.RequirementFlag.out_of_sequence_detection]
        if self.delegate:
            flags.append(gssapi.RequirementFlag.delegate_to_peer)
        if self.mutual_authentication != MUTUAL_DISABLED:
            flags.append(gssapi.RequirementFlag.mutual_authentication)
        try:
            return gssapi.SecurityContext(
                name=self._resolve_target_name(request_host),
                creds=self.creds,
                mech=gssapi.OID.from_int_seq("1.3.6.1.5.5.2"),  # SPNEGO
                flags=flags,
                usage="initiate",
            )
        except gssapi.exceptions.GSSError as exc:
            raise SPNEGOExchangeError(f"failed to initialize GSSAPI context: {exc}") from exc

    @staticmethod
    def _step(context: typing.Any, in_token: typing.Optional[bytes]) -> typing.Optional[bytes]:
        gssapi = _import_gssapi()
        try:
            return context.step(in_token)
        except gssapi.exceptions.GSSError as exc:
            raise SPNEGOExchangeError(f"GSSAPI token exchange failed: {exc}") from exc

    @staticmethod
    def _extract_negotiate_token(response: httpx2.Response) -> typing.Optional[bytes]:
        auth_header = response.headers.get("WWW-Authenticate", "")
        match = _NEGOTIATE_TOKEN_PATTERN.search(auth_header)
        if match is None:
            return None
        token = match.group(1)
        return base64.b64decode(token) if token else b""

    @staticmethod
    def _has_negotiate_challenge(response: httpx2.Response) -> bool:
        return "negotiate" in response.headers.get("WWW-Authenticate", "").lower()

    def _set_header(self, request: httpx2.Request, out_token: bytes) -> None:
        request.headers["Authorization"] = "Negotiate " + base64.b64encode(out_token).decode()

    def auth_flow(
        self, request: httpx2.Request
    ) -> typing.Generator[httpx2.Request, httpx2.Response, None]:
        host = request.url.host
        context = None

        if self.opportunistic_auth:
            context = self._make_context(host)
            out_token = self._step(context, None)
            if out_token:
                self._set_header(request, out_token)

        response = yield request

        challenges_answered = 0
        while (
            response.status_code == 401
            and self._has_negotiate_challenge(response)
            and challenges_answered < _MAX_CHALLENGES
        ):
            in_token = self._extract_negotiate_token(response)
            if context is None:
                context = self._make_context(host)
            out_token = self._step(context, in_token or None)
            if out_token is None:
                break
            self._set_header(request, out_token)
            response = yield request
            challenges_answered += 1

        if self.mutual_authentication != MUTUAL_DISABLED and not response.is_error:
            self._verify_mutual_auth(context, response)

    def _verify_mutual_auth(self, context: typing.Any, response: httpx2.Response) -> None:
        in_token = self._extract_negotiate_token(response)
        if not in_token:
            if self.mutual_authentication == MUTUAL_REQUIRED:
                raise SPNEGOExchangeError(
                    "mutual authentication required but the server sent no Negotiate token"
                )
            return
        if context is None:
            raise SPNEGOExchangeError(
                "mutual authentication required but no GSSAPI exchange took place"
            )
        self._step(context, in_token)
        if not context.complete:
            raise SPNEGOExchangeError("mutual authentication failed: incomplete GSSAPI context")
