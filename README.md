Trino Python client
===================
A [Trino](https://trino.io/) client for the [Python](https://www.python.org/) programming language.
It supports Python>=3.7 and pypy.

[![Build Status](https://github.com/trinodb/trino-python-client/workflows/ci/badge.svg)](https://github.com/trinodb/trino-python-client/actions?query=workflow%3Aci+event%3Apush+branch%3Amaster)
[![Trino Slack](https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358)](https://trino.io/slack.html)
[![Trino: The Definitive Guide book download](https://img.shields.io/badge/Trino%3A%20The%20Definitive%20Guide-download-brightgreen)](https://www.starburst.io/info/oreilly-trino-guide/)

## Usage

### The Python Database API (DBAPI)

**Installation**

```
$ pip install trino
```

**Quick Start**

Use the DBAPI interface to query Trino:

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

- SQLAlchemy >= 1.3
- Trino server >= 351

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

## Authentications

### Basic Authentication

The `BasicAuthentication` class can be used to connect to a Trino cluster configured with
the [Password file authentication type, LDAP authentication type or Salesforce authentication type](https://trino.io/docs/current/security/authentication-types.html):

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
    
    # or
    from trino.auth import BasicAuthentication
    engine = create_engine(
        "trino://<username>@<host>:<port>/<catalog>",
        connect_args={
            "auth": BasicAuthentication("<username>", "<password>"),
            "http_scheme": "https",
        }
    )
    ```

### JWT Authentication

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
  
    # or
    from trino.auth import JWTAuthentication
    engine = create_engine(
        "trino://<username>@<host>:<port>/<catalog>",
        connect_args={
            "auth": JWTAuthentication("<jwt_token>"),
            "http_scheme": "https",
        }
    )
    ```

### OAuth2 Authentication

- `OAuth2Authentication` class can be used to connect to a Trino cluster configured with
the [OAuth2 authentication type](https://trino.io/docs/current/security/oauth2.html).
- A callback to handle the redirect url can be provided via param `redirect_auth_url_handler`, by default it just outputs the redirect url to stdout.

* DBAPI

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

* SQLAlchemy

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

### Certificate Authentication

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

    engine = create_engine(
    "trino://<username>@<host>:<port>/<catalog>",
        connect_args={
            "auth": CertificateAuthentication("/path/to/cert.pem", "/path/to/key.pem"),
            "http_scheme": "https",
        }
    )
    ```

### Kerberos Authentication

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

### User impersonation

In the case of user who submit the query is not the same as user who authenticate to Trino server (e.g in Superset),
you can set `username` to different from `principal_id`. Note that `principal_id` is extracted from `auth`,
for example `username` in BasicAuthentication, `sub` in JWT token or `service-name` in KerberosAuthentication and
please make sure that [`principal_id` has permission to impersonate `username`](https://trino.io/docs/current/security/file-system-access-control.html#impersonation-rules).

### Extra Credential

Send [`extra credentials`](https://trino.io/docs/current/develop/client-protocol.html#client-request-headers):

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

# Improved Python types

If you enable the flag `experimental_python_types`, the client will convert the results of the query to the 
corresponding Python types. For example, if the query returns a `DECIMAL` column, the result will be a `Decimal` object.

Limitations of the Python types are described in the 
[Python types documentation](https://docs.python.org/3/library/datatypes.html). These limitations will generate an 
exception `trino.exceptions.DataError` if the query returns a value that cannot be converted to the corresponding Python 
type.

```python
import trino
import pytz
from datetime import datetime

conn = trino.dbapi.connect(
    ...
)

cur = conn.cursor(experimental_python_types=True)

params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=pytz.timezone('America/Los_Angeles'))

cur.execute("SELECT ?", params=(params,))
rows = cur.fetchall()

assert rows[0][0] == params
assert cur.description[0][1] == "timestamp with time zone"
```

# Development

## Getting Started With Development

Start by forking the repository and then modify the code in your fork.

Clone the repository and go inside the code directory. Then you can get the
version with `./setup.py --version`.

We recommend that you use Python3's `venv` for development:

```
$ python3 -m venv .venv
$ . .venv/bin/activate
$ pip install .
```

For development purpose, pip can reference the code you are modifying in a
*virtual env*:

```
$ pip install -e .
# To additionally install all dependencies for development run below command
$ pip install -e '.[tests]'
```

That way, you do not need to run `pip install` again to make your changes
applied to the *virtual env*.

When the code is ready, submit a Pull Request.

### Code Style

- For Python code, adhere to PEP 8.
- Prefer code that is readable over one that is "clever".
- When writing a Git commit message, follow these [guidelines](https://chris.beams.io/posts/git-commit/).

### Running Tests

`trino-python-client` uses [pytest](https://pytest.org/) for its tests. To run
only unit tests, type:

```
$ pytest tests/unit
```

Then you can pass options like `--pdb` or anything supported by `pytest --help`.

To run the tests with different versions of Python in managed *virtual envs*,
use `tox` (see the configuration in `tox.ini`):

```
$ tox
```

To run integration tests:

```
$ pytest tests/integration
```

They pull a Docker image and then run a container with a Trino server:
- the image is named `trinodb/trino:${TRINO_VERSION}`
- the container is named `trino-python-client-tests-{uuid4()[:7]}`

### Releasing

- [Set up your development environment](#Getting-Started-With-Development).
- Check the local workspace is up to date and has no uncommitted changes
  ```bash
  git fetch -a && git status
  ```
- Change version in `trino/__init__.py` to a new version, e.g. `0.123.0`.
- Commit
  ```bash
  git commit -a -m "Bump version to 0.123.0"
  ```
- Create an annotated tag
  ```bash
  git tag -m "" 0.123.0
  ```
- Create release package and upload it to PyPI
  ```bash
  . .venv/bin/activate &&
  pip install twine &&
  rm -rf dist/ &&
  ./setup.py sdist bdist_wheel &&
  twine upload dist/* &&
  open https://pypi.org/project/trino/ &&
  echo "Released!"
  ```
- Push the branch and the tag
  ```bash
  git push upstream master 0.123.0
  ```
- Send release announcement.

## Need Help?

Feel free to create an issue as it make your request visible to other users and contributors.

If an interactive discussion would be better or if you just want to hangout and chat about
the Trino Python client, you can join us on the *#python-client* channel on
[Trino Slack](https://trino.io/slack.html).
