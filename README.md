# Trino Python client

Client for [Trino](https://trino.io/), a distributed SQL engine for interactive and batch big data processing.
Provides a low-level client and a DBAPI 2.0 implementation and a SQLAlchemy adapter.
It supports Python>=3.7 and PyPy.

[![Build Status](https://github.com/trinodb/trino-python-client/workflows/ci/badge.svg)](https://github.com/trinodb/trino-python-client/actions?query=workflow%3Aci+event%3Apush+branch%3Amaster)
[![Trino Slack](https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358)](https://trino.io/slack.html)
[![Trino: The Definitive Guide book download](https://img.shields.io/badge/Trino%3A%20The%20Definitive%20Guide-download-brightgreen)](https://www.starburst.io/info/oreilly-trino-guide/)

## Development

See [DEVELOPMENT](.github/DEVELOPMENT.md) for information about code style,
development process, and guidelines.

See [CONTRIBUTING](.github/CONTRIBUTING.md) for contribution requirements.

## Usage

### The Python Database API (DBAPI)

**Installation**

```
$ pip install trino
```

**Quick Start**

Use the DBAPI interface to query Trino:

if `host` is a valid url, the port and http schema will be automatically determined. For example `https://my-trino-server:9999` will assign the `http_schema` property to `https` and port to `9999`.

```python
from trino.dbapi import connect

conn = connect(
    host="<host>",
    port=<port>,
    user="<username>",
    catalog="<catalog>",
    schema="<schema>",
)
cur = conn.cursor()
cur.execute("SELECT * FROM system.runtime.nodes")
rows = cur.fetchall()
```

This will query the `system.runtime.nodes` system tables that shows the nodes
in the Trino cluster.

The DBAPI implementation in `trino.dbapi` provides methods to retrieve fewer
rows for example `Cursor.fetchone()` or `Cursor.fetchmany()`. By default
`Cursor.fetchmany()` fetches one row. Please set
`trino.dbapi.Cursor.arraysize` accordingly.

### SQLAlchemy

**Prerequisite**

- Trino server >= 351

**Compatibility**

`trino.sqlalchemy` is compatible with the latest 1.3.x, 1.4.x and 2.0.x SQLAlchemy
versions at the time of release of a particular version of the client.

**Installation**

```
$ pip install trino[sqlalchemy]
```

**Usage**

To connect to Trino using SQLAlchemy, use a connection string (URL) following this pattern:

```
trino://<username>:<password>@<host>:<port>/<catalog>/<schema>
```

NOTE: `password` and `schema` are optional

**Examples**:

```python
from sqlalchemy import create_engine
from sqlalchemy.schema import Table, MetaData
from sqlalchemy.sql.expression import select, text

engine = create_engine('trino://user@localhost:8080/system')
connection = engine.connect()

rows = connection.execute(text("SELECT * FROM runtime.nodes")).fetchall()

# or using SQLAlchemy schema
nodes = Table(
    'nodes',
    MetaData(schema='runtime'),
    autoload=True,
    autoload_with=engine
)
rows = connection.execute(select(nodes)).fetchall()
```

In order to pass additional connection attributes use [connect_args](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine.params.connect_args) method.
Attributes can also be passed in the connection string.

```python
from sqlalchemy import create_engine
from trino.sqlalchemy import URL

engine = create_engine(
    URL(
        host="localhost",
        port=8080,
        catalog="system"
    ),
    connect_args={
      "session_properties": {'query_max_run_time': '1d'},
      "client_tags": ["tag1", "tag2"],
      "roles": {"catalog1": "role1"},
    }
)

# or in connection string
engine = create_engine(
    'trino://user@localhost:8080/system?'
    'session_properties={"query_max_run_time": "1d"}'
    '&client_tags=["tag1", "tag2"]'
    '&roles={"catalog1": "role1"}'
)

# or using the URL factory method
engine = create_engine(URL(
  host="localhost",
  port=8080,
  client_tags=["tag1", "tag2"]
))
```

## Authentication mechanisms

### Basic authentication

The `BasicAuthentication` class can be used to connect to a Trino cluster configured with
the [Password file, LDAP or Salesforce authentication type](https://trino.io/docs/current/security/authentication-types.html):

- DBAPI

    ```python
    from trino.dbapi import connect
    from trino.auth import BasicAuthentication

    conn = connect(
        user="<username>",
        auth=BasicAuthentication("<username>", "<password>"),
        http_scheme="https",
        ...
    )
    ```

- SQLAlchemy

    ```python
    from sqlalchemy import create_engine

    engine = create_engine("trino://<username>:<password>@<host>:<port>/<catalog>")

    # or as connect_args
    from trino.auth import BasicAuthentication
    engine = create_engine(
        "trino://<username>@<host>:<port>/<catalog>",
        connect_args={
            "auth": BasicAuthentication("<username>", "<password>"),
            "http_scheme": "https",
        }
    )
    ```

### JWT authentication

The `JWTAuthentication` class can be used to connect to a Trino cluster configured with
the [`JWT` authentication type](https://trino.io/docs/current/security/jwt.html):

- DBAPI

    ```python
    from trino.dbapi import connect
    from trino.auth import JWTAuthentication

    conn = connect(
        user="<username>",
        auth=JWTAuthentication("<jwt_token>"),
        http_scheme="https",
        ...
    )
    ```

- SQLAlchemy

    ```python
    from sqlalchemy import create_engine

    engine = create_engine("trino://<username>@<host>:<port>/<catalog>/<schema>?access_token=<jwt_token>")

    # or as connect_args
    from trino.auth import JWTAuthentication
    engine = create_engine(
        "trino://<username>@<host>:<port>/<catalog>",
        connect_args={
            "auth": JWTAuthentication("<jwt_token>"),
            "http_scheme": "https",
        }
    )
    ```

### OAuth2 authentication

The `OAuth2Authentication` class can be used to connect to a Trino cluster configured with
the [OAuth2 authentication type](https://trino.io/docs/current/security/oauth2.html).

A callback to handle the redirect url can be provided via param `redirect_auth_url_handler` of the `trino.auth.OAuth2Authentication` class. By default, it will try to launch a web browser (`trino.auth.WebBrowserRedirectHandler`) to go through the authentication flow and output the redirect url to stdout (`trino.auth.ConsoleRedirectHandler`). Multiple redirect handlers are combined using the `trino.auth.CompositeRedirectHandler` class.

The OAuth2 token will be cached either per `trino.auth.OAuth2Authentication` instance or, when keyring is installed, it will be cached within a secure backend (MacOS keychain, Windows credential locker, etc) under a key including host of the Trino connection. Keyring can be installed using `pip install 'trino[external-authentication-token-cache]'`.

- DBAPI

    ```python
    from trino.dbapi import connect
    from trino.auth import OAuth2Authentication

    conn = connect(
        user="<username>",
        auth=OAuth2Authentication(),
        http_scheme="https",
        ...
    )
    ```

- SQLAlchemy

    ```python
    from sqlalchemy import create_engine
    from trino.auth import OAuth2Authentication

    engine = create_engine(
    "trino://<username>@<host>:<port>/<catalog>",
        connect_args={
            "auth": OAuth2Authentication(),
            "http_scheme": "https",
        }
    )
    ```

### Certificate authentication

`CertificateAuthentication` class can be used to connect to Trino cluster configured with [certificate based authentication](https://trino.io/docs/current/security/certificate.html). `CertificateAuthentication` requires paths to a valid client certificate and private key.

- DBAPI

    ```python
    from trino.dbapi import connect
    from trino.auth import CertificateAuthentication

    conn = connect(
        user="<username>",
        auth=CertificateAuthentication("/path/to/cert.pem", "/path/to/key.pem"),
        http_scheme="https",
        ...
    )
    ```

- SQLAlchemy

    ```python
    from sqlalchemy import create_engine
    from trino.auth import CertificateAuthentication

    engine = create_engine("trino://<username>@<host>:<port>/<catalog>/<schema>?cert=<cert>&key=<key>")

    # or as connect_args
    engine = create_engine(
    "trino://<username>@<host>:<port>/<catalog>",
        connect_args={
            "auth": CertificateAuthentication("/path/to/cert.pem", "/path/to/key.pem"),
            "http_scheme": "https",
        }
    )
    ```

### Kerberos authentication

The `KerberosAuthentication` class can be used to connect to a Trino cluster configured with
the [`Kerberos` authentication type](https://trino.io/docs/current/security/kerberos.html):

- DBAPI

    ```python
    from trino.dbapi import connect
    from trino.auth import KerberosAuthentication

    conn = connect(
        user="<username>",
        auth=KerberosAuthentication(...),
        http_scheme="https",
        ...
    )
    ```

- SQLAlchemy

    ```python
    from sqlalchemy import create_engine
    from trino.auth import KerberosAuthentication

    engine = create_engine(
        "trino://<username>@<host>:<port>/<catalog>",
        connect_args={
            "auth": KerberosAuthentication(...),
            "http_scheme": "https",
        }
    )
    ```

## User impersonation

In the case where user who submits the query is not the same as user who authenticates to Trino server (e.g in Superset),
you can set `username` to be different from `principal_id`. Note that `principal_id` is extracted from `auth`,
for example `username` in BasicAuthentication, `sub` in JWT token or `service-name` in KerberosAuthentication.
You need to make sure that [`principal_id` has permission to impersonate `username`](https://trino.io/docs/current/security/file-system-access-control.html#impersonation-rules).

### Extra credentials

[`Extra credentials`](https://trino.io/docs/current/develop/client-protocol.html#client-request-headers) can be sent as:

```python
import trino
conn = trino.dbapi.connect(
    host='localhost',
    port=443,
    user='the-user',
    extra_credential=[('a.username', 'bar'), ('a.password', 'foo')],
)

cur = conn.cursor()
cur.execute('SELECT * FROM system.runtime.nodes')
rows = cur.fetchall()
```

## Roles

Authorization roles to use for catalogs, specified as a dict with key-value pairs for the catalog and role. For example, `{"catalog1": "roleA", "catalog2": "roleB"}` sets `roleA` for `catalog1` and `roleB` for `catalog2`. See Trino docs.

```python
import trino
conn = trino.dbapi.connect(
    host='localhost',
    port=443,
    user='the-user',
    roles={"catalog1": "roleA", "catalog2": "roleB"},
)
```

You could also pass `system` role without explicitly specifing "system" catalog:

```python
import trino
conn = trino.dbapi.connect(
    host='localhost',
    port=443,
    user='the-user',
    roles="role1" # equivalent to {"system": "role1"}
)
```

## Timezone

The time zone for the session can be explicitly set using the IANA time zone
name. When not set the time zone defaults to the client side local timezone.

```python
import trino
conn = trino.dbapi.connect(
    host='localhost',
    port=443,
    user='username',
    timezone='Europe/Brussels',
)
```

> **NOTE: The behaviour till version 0.320.0 was the same as setting session timezone to UTC.**
> **To preserve that behaviour pass `timezone='UTC'` when creating the connection.**

## SSL

### SSL verification

In order to disable SSL verification, set the `verify` parameter to `False`.

```python
from trino.dbapi import connect
from trino.auth import BasicAuthentication

conn = connect(
    user="<username>",
    auth=BasicAuthentication("<username>", "<password>"),
    http_scheme="https",
    verify=False
)
```

### Self-signed certificates

To use self-signed certificates, specify a path to the certificate in `verify` parameter.
More details can be found in [the Python requests library documentation](https://requests.readthedocs.io/en/latest/user/advanced/#ssl-cert-verification).

```python
from trino.dbapi import connect
from trino.auth import BasicAuthentication

conn = connect(
    user="<username>",
    auth=BasicAuthentication("<username>", "<password>"),
    http_scheme="https",
    verify="/path/to/cert.crt"
)
```

## Transactions

The client runs by default in *autocommit* mode. To enable transactions, set
*isolation_level* to a value different than `IsolationLevel.AUTOCOMMIT`:

```python
from trino.dbapi import connect
from trino.transaction import IsolationLevel

with connect(
        isolation_level=IsolationLevel.REPEATABLE_READ,
        ...
) as conn:
    cur = conn.cursor()
    cur.execute('INSERT INTO sometable VALUES (1, 2, 3)')
    cur.fetchall()
    cur.execute('INSERT INTO sometable VALUES (4, 5, 6)')
    cur.fetchall()
```

The transaction is created when the first SQL statement is executed.
`trino.dbapi.Connection.commit()` will be automatically called when the code
exits the *with* context and the queries succeed, otherwise
`trino.dbapi.Connection.rollback()` will be called.

## Legacy Primitive types

By default, the client will convert the results of the query to the
corresponding Python types. For example, if the query returns a `DECIMAL` column, the result will be a `Decimal` object.
If you want to disable this behaviour, set flag `legacy_primitive_types` to `True`.

Limitations of the Python types are described in the 
[Python types documentation](https://docs.python.org/3/library/datatypes.html). These limitations will generate an 
exception `trino.exceptions.TrinoDataError` if the query returns a value that cannot be converted to the corresponding Python 
type.

```python
import trino

conn = trino.dbapi.connect(
    legacy_primitive_types=True,
    ...
)

cur = conn.cursor()
# Negative DATE cannot be represented with Python types
# legacy_primitive_types needs to be enabled
cur.execute("SELECT DATE '-2001-08-22'")
rows = cur.fetchall()

assert rows[0][0] == "-2001-08-22"
assert cur.description[0][1] == "date"
```

### Trino to Python type mappings

| Trino type | Python type       |
|------------|-------------------|
| BOOLEAN    | bool              |
| TINYINT    | int               |
| SMALLINT   | int               |
| INTEGER    | int               |
| BIGINT     | int               |
| REAL       | float             |
| DOUBLE     | float             |
| DECIMAL    | decimal.Decimal   |
| VARCHAR    | str               |
| CHAR       | str               |
| VARBINARY  | bytes             |
| DATE       | datetime.date     |
| TIME       | datetime.time     |
| TIMESTAMP  | datetime.datetime |
| ARRAY      | list              |
| MAP        | dict              |
| ROW        | tuple             |

Trino types other than those listed above are not mapped to Python types. To use those use [legacy primitive types](#legacy-primitive-types).

# Need help?

Feel free to create an issue as it makes your request visible to other users and contributors.

If an interactive discussion would be better or if you just want to hangout and chat about
the Trino Python client, you can join us on the *#python-client* channel on
[Trino Slack](https://trino.io/slack.html).
