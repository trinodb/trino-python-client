import json
import re
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from urllib.parse import quote_plus
from urllib.parse import urlparse

from sqlalchemy import exc


def _rfc_1738_quote(text):
    return re.sub(r"[:@/]", lambda m: "%%%X" % ord(m.group(0)), text)


def _assert_valid_host(host: str) -> None:
    # Parse with a leading "//" so a bare hostname is treated as the authority
    # rather than a path. `urlparse(host).scheme` catches an embedded scheme.
    parsed = urlparse("//" + host)
    try:
        has_port = parsed.port is not None
    except ValueError:
        # A malformed port raises here.
        has_port = True
    if urlparse(host).scheme or has_port or parsed.path or parsed.username or parsed.password:
        raise exc.ArgumentError(
            f"host must be a hostname only, without a scheme, port, path, or credentials. Got {host!r}. "
            "For example, use 'example.com' instead of 'https://example.com' and pass the port via the "
            "'port' argument."
        )


def _url(
    host: str,
    port: Optional[int] = 8080,
    user: Optional[str] = None,
    password: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    source: Optional[str] = "trino-sqlalchemy",
    session_properties: Dict[str, str] = None,
    http_headers: Dict[str, Union[str, int]] = None,
    extra_credential: Optional[List[Tuple[str, str]]] = None,
    client_tags: Optional[List[str]] = None,
    legacy_primitive_types: Optional[bool] = None,
    legacy_prepared_statements: Optional[bool] = None,
    access_token: Optional[str] = None,
    cert: Optional[str] = None,
    key: Optional[str] = None,
    verify: Optional[bool] = None,
    roles: Optional[Dict[str, str]] = None
) -> str:
    """
    Composes a SQLAlchemy connection string from the given database connection
    parameters.
    Parameters containing special characters (e.g., '@', '%') need to be encoded to be parsed correctly.
    """

    trino_url = "trino://"

    if user is not None:
        trino_url += _rfc_1738_quote(user)

    if password is not None:
        if user is None:
            raise exc.ArgumentError("user must be specified when specifying a password.")
        trino_url += f":{_rfc_1738_quote(password)}"

    if user is not None:
        trino_url += "@"

    if not host:
        raise exc.ArgumentError("host must be specified.")

    _assert_valid_host(host)

    trino_url += host

    if not port:
        raise exc.ArgumentError("port must be specified.")

    trino_url += f":{port}/"

    if catalog is not None:
        trino_url += f"{quote_plus(catalog)}"

    if schema is not None:
        if catalog is None:
            raise exc.ArgumentError("catalog must be specified when specifying a default schema.")
        trino_url += f"/{quote_plus(schema)}"

    assert source
    trino_url += f"?source={quote_plus(source)}"

    if session_properties is not None:
        trino_url += f"&session_properties={quote_plus(json.dumps(session_properties))}"

    if http_headers is not None:
        trino_url += f"&http_headers={quote_plus(json.dumps(http_headers))}"

    if extra_credential is not None:
        # repr is used here as json.dumps converts tuples into arrays
        trino_url += f"&extra_credential={quote_plus(json.dumps(extra_credential))}"

    if client_tags is not None:
        trino_url += f"&client_tags={quote_plus(json.dumps(client_tags))}"

    if legacy_primitive_types is not None:
        trino_url += f"&legacy_primitive_types={json.dumps(legacy_primitive_types)}"

    if legacy_prepared_statements is not None:
        trino_url += f"&legacy_prepared_statements={json.dumps(legacy_prepared_statements)}"

    if access_token is not None:
        trino_url += f"&access_token={quote_plus(access_token)}"

    if cert is not None:
        trino_url += f"&cert={quote_plus(cert)}"

    if key is not None:
        trino_url += f"&key={quote_plus(key)}"

    if verify is not None:
        trino_url += f"&verify={json.dumps(verify)}"

    if roles is not None:
        trino_url += f"&roles={quote_plus(json.dumps(roles))}"

    return trino_url
