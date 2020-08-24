[![Build Status](https://github.com/prestosql/presto-python-client/workflows/ci/badge.svg)](https://github.com/prestosql/presto-python-client/actions?query=workflow%3Aci+event%3Apush+branch%3Amaster)
[![Presto Slack](https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358)](https://prestosql.io/slack.html)
[![Presto: The Definitive Guide book download](https://img.shields.io/badge/Presto%3A%20The%20Definitive%20Guide-download-brightgreen)](https://www.starburstdata.com/oreilly-presto-guide-download/)

# Introduction

This package provides a client interface to query [Presto](https://prestosql.io/)
a distributed SQL engine. It supports Python 2.7, 3.5, 3.6, and pypy.

# Installation

```
$ pip install presto-client
```

# Quick Start

Use the DBAPI interface to query Presto:

```python
import presto
conn = presto.dbapi.connect(
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
in the Presto cluster.

The DBAPI implementation in `presto.dbapi` provides methods to retrieve fewer
rows for example `Cursorfetchone()` or `Cursor.fetchmany()`. By default
`Cursor.fetchmany()` fetches one row. Please set
`presto.dbapi.Cursor.arraysize` accordingly.

# Basic Authentication
The `BasicAuthentication` class can be used to connect to a LDAP-configured Presto
cluster:
```python
import presto
conn = presto.dbapi.connect(
    host='coordinator url',
    port=8443,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
    http_scheme='https',
    auth=presto.auth.BasicAuthentication("principal id", "password"),
)
cur = conn.cursor()
cur.execute('SELECT * FROM system.runtime.nodes')
rows = cur.fetchall()
```

# Transactions
The client runs by default in *autocommit* mode. To enable transactions, set
*isolation_level* to a value different than `IsolationLevel.AUTOCOMMIT`:

```python
import presto
from presto import transaction
with presto.dbapi.connect(
    host='localhost',
    port=8080,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
    isolation_level=transaction.IsolationLevel.REPEATABLE_READ,
) as conn:
  cur = conn.cursor()
  cur.execute('INSERT INTO sometable VALUES (1, 2, 3)')
  cur.execute('INSERT INTO sometable VALUES (4, 5, 6)')
```

The transaction is created when the first SQL statement is executed.
`presto.dbapi.Connection.commit()` will be automatically called when the code
exits the *with* context and the queries succeed, otherwise
`presto.dbapi.Connection.rollback()' will be called.

# Running Tests

There is a helper scripts, `run`, that provides commands to run tests.
Type `./run tests` to run both unit and integration tests.

`presto-python-client` uses [pytest](https://pytest.org/) for its tests. To run
only unit tests, type:

```
$ pytest tests
```

Then you can pass options like `--pdb` or anything supported by `pytest --help`.

To run the tests with different versions of Python in managed *virtualenvs*,
use `tox` (see the configuration in `tox.ini`):

```
$ tox
```

To run integration tests:

```
$ pytest integration_tests
```

They pull a Docker image and then run a container with a Presto server:
- the image is named `prestosql/presto:${PRESTO_VERSION}`
- the container is named `presto-python-client-tests-{uuid4()[:7]}`

# Development

## Getting Started With Development

Start by forking the repository and then modify the code in your fork.

Clone the repository and go inside the code directory. Then you can get the
version with `./setup.py --version`.

We recommend that you use `virtualenv` for development:

```
$ virtualenv /path/to/env
$ /path/to/env/bin/active
$ pip install -r requirements.txt
```

For development purpose, pip can reference the code you are modifying in a
*virtualenv*:

```
$ pip install -e .[tests]
```

That way, you do not need to run `pip install` again to make your changes
applied to the *virtualenv*.

When the code is ready, submit a Pull Request.

## Code Style

- For Python code, adhere to PEP 8.
- Prefer code that is readable over one that is "clever".
- When writing a Git commit message, follow these [guidelines](https://chris.beams.io/posts/git-commit/).

# Need Help?

Feel free to create an issue as it make your request visible to other users and contributors.

If an interactive discussion would be better or if you just want to hangout and chat about
the Presto Python client, you can join us on the *#python-client* channel on
[Presto Slack](https://prestosql.io/slack.html).
