# Introduction

This package provides a client interface to query [Presto](https://prestodb.io/)
a distributed SQL engine. It supports Python 2.7, 3.5, 3.6, and pypy.

# Installation

```
$ pip install presto-python-client
```

# Quick Start

Use the DBAPI interface to query Presto:

```python
import prestodb
conn=prestodb.dbapi.Connection(
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

The DBAPI implementation in `prestodb.dbapi` provides methods to retrieve fewer
rows for example `Cursorfetchone()` or `Cursor.fetchmany()`. By default
`Cursor.fetchmany()` fetches one row. Please set
`prestodb.dbapi.Cursor.arraysize` accordingly.

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

They build a `Docker` image and then run a container with a Presto server:
- the image is named `presto-server:${PRESTO_VERSION}`
- the container is named `presto-python-client-tests-{uuid4()[:7]}`

The container is expected to be removed after the tests are finished.

Please refer to the `Dockerfile` for details. You will find the configuration
in `etc/`.

You can use `./run` to manipulate the containers:

- `./run presto_server`: build and run Presto in a container
- `./run presto_cli CONTAINER_ID`: connect the Java Presto CLI to a container
- `./run list`: list the running containers
- `./run clean`: kill the containers

# Development

Start by forking the repository and then modify the code in your fork.
Please refer to `CONTRIBUTING.md` before submitting your contributions.

Clone the repository and go inside the code directory. Then you can get the
version with `python setup.py --version`.

We recommend that you use `virtualenv` to develop on `presto-python-client`:

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
