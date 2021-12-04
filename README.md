[![Build Status](https://github.com/trinodb/trino-python-client/workflows/ci/badge.svg)](https://github.com/trinodb/trino-python-client/actions?query=workflow%3Aci+event%3Apush+branch%3Amaster)
[![Trino Slack](https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358)](https://trino.io/slack.html)
[![Trino: The Definitive Guide book download](https://img.shields.io/badge/Trino%3A%20The%20Definitive%20Guide-download-brightgreen)](https://www.starburst.io/info/oreilly-trino-guide/)

# Introduction

This package provides a client interface to query [Trino](https://trino.io/)
a distributed SQL engine. It supports Python>=3.6 and pypy.

# Installation

```
$ pip install trino
```

# Quick Start

Use the DBAPI interface to query Trino:

```python
import trino
conn = trino.dbapi.connect(
    host='localhost',
    port=8080,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
)
cur = conn.cursor()
cur.execute('SELECT * FROM system.runtime.nodes')
rows = cur.fetchall()
```

This will query the `system.runtime.nodes` system tables that shows the nodes
in the Trino cluster.

The DBAPI implementation in `trino.dbapi` provides methods to retrieve fewer
rows for example `Cursor.fetchone()` or `Cursor.fetchmany()`. By default
`Cursor.fetchmany()` fetches one row. Please set
`trino.dbapi.Cursor.arraysize` accordingly.

# Basic Authentication
The `BasicAuthentication` class can be used to connect to a Trino cluster configured with
the [Password file authentication type, LDAP authentication type or Salesforce authentication type](https://trino.io/docs/current/security/authentication-types.html):

```python
import trino
conn = trino.dbapi.connect(
    host='coordinator url',
    port=8443,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
    http_scheme='https',
    auth=trino.auth.BasicAuthentication("principal id", "password"),
)
cur = conn.cursor()
cur.execute('SELECT * FROM system.runtime.nodes')
rows = cur.fetchall()
```

# JWT Authentication
The `JWTAuthentication` class can be used to connect to a Trino cluster configured with
the [`JWT` authentication type](https://trino.io/docs/current/security/authentication-types.html):
```python
import trino
JWT_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiSm9obiBEb2UifQ.DjwRE2jZhren2Wt37t5hlVru6Myq4AhpGLiiefF69u8'
conn = trino.dbapi.connect(
    host='coordinator-url',
    port=8443,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
    http_scheme='https',
    auth=trino.auth.JWTAuthentication(JWT_TOKEN),
)
cur = conn.cursor()
cur.execute('SELECT * FROM system.runtime.nodes')
rows = cur.fetchall()
```
# OAuth2 Authentication
- `OAuth2Authentication` class can be used to connect to a Trino cluster configured with
the [OAUTH2 authentication type](https://trino.io/docs/current/security/authentication-types.html):
- A callback to handle the redirect url can be provided via param redirect_auth_url_handler, by default it just outputs the redirect url to stdout
```python
import trino
conn = trino.dbapi.connect(
    host='coordinator-url',
    port=8443,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
    http_scheme='https',
    auth=trino.auth.OAuth2Authentication(),
)
cur = conn.cursor()
cur.execute('SELECT * FROM system.runtime.nodes')
rows = cur.fetchall()
```

# Transactions
The client runs by default in *autocommit* mode. To enable transactions, set
*isolation_level* to a value different than `IsolationLevel.AUTOCOMMIT`:

```python
import trino
from trino import transaction
with trino.dbapi.connect(
    host='localhost',
    port=8080,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
    isolation_level=transaction.IsolationLevel.REPEATABLE_READ,
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

## Code Style

- For Python code, adhere to PEP 8.
- Prefer code that is readable over one that is "clever".
- When writing a Git commit message, follow these [guidelines](https://chris.beams.io/posts/git-commit/).

## Running Tests

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

## Releasing

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

# Need Help?

Feel free to create an issue as it make your request visible to other users and contributors.

If an interactive discussion would be better or if you just want to hangout and chat about
the Trino Python client, you can join us on the *#python-client* channel on
[Trino Slack](https://trino.io/slack.html).
