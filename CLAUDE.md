# CLAUDE.md

## Common Commands

```bash
# Setup
python3 -m venv .venv && . .venv/bin/activate
pip install -e '.[tests]'

# Run unit tests
pytest tests/unit

# Run a single test
pytest tests/unit/test_client.py::test_name -sv

# Run integration tests (starts a Trino Docker container automatically, or uses an existing one on localhost:8080)
pytest tests/integration

# Start a standalone Trino (+ LocalStack) dev server for manual testing, without pytest
python tests/development_server.py

# Lint and format checks
pre-commit run --all-files

# Type checking only
mypy trino/

# Run tests across all supported Python versions
tox
```

## Architecture

### Package layout (`trino/`)

- **`client.py`** - Core HTTP protocol implementation. `TrinoRequest` manages HTTP requests to the coordinator, `TrinoQuery` manages query lifecycle (submit -> poll -> fetch results), `ClientSession` holds connection state (headers, session properties, transaction ID). Implements the protocol described at https://github.com/trinodb/trino/wiki/HTTP-Protocol.
- **`dbapi.py`** - PEP 249 DBAPI 2.0 interface. `Connection` and `Cursor` classes that wrap `client.py`. Entry point is `trino.dbapi.connect()`.
- **`sqlalchemy/`** - SQLAlchemy dialect (`TrinoDialect`), compiler, and type mapping. Registered as `trino://` via `sqlalchemy.dialects` entry point. Compatible with SQLAlchemy 1.3.x, 1.4.x, and 2.0.x.
- **`auth.py`** - Authentication implementations (Basic, JWT, OAuth2, Kerberos/GSSAPI, Certificate).
- **`types.py`** - Python representations of Trino SQL types.
- **`mapper.py`** - Conversion between Trino wire types and Python objects.
- **`transaction.py`** - Transaction and isolation level management.
- **`exceptions.py`** - Exception hierarchy following DBAPI 2.0 spec.
- **`constants.py`** - Protocol header names and type metadata constants.
- **`logging.py`** - Logger factory used across the package.

### Tests (`tests/`)

- `tests/unit/` - No external dependencies needed. Uses `httpretty` for HTTP mocking.
- `tests/integration/` - Requires Docker. Tests automatically pull `trinodb/trino` image and start a container (or reuse one on port 8080).
  - Env vars: `TRINO_VERSION` (image tag, default `latest`), `TRINO_RUNNING_HOST`/`TRINO_RUNNING_PORT` (use an existing server instead of starting one).
  - When `TRINO_VERSION` is `latest` or >= 466, a LocalStack container with an S3 `spooling` bucket is also started to test the spooled client protocol; older versions use the `etc/*-pre-466*` configs.

### Key dependencies

- `requests` - HTTP transport
- `orjson` (CPython) / `json` (PyPy) - JSON parsing, selected at import time in `client.py`
- `lz4`, `zstandard` - Response decompression
- `python-dateutil`, `pytz`, `tzlocal` - Timezone/datetime handling

## Code Style

- **Max line length**: 120 characters (flake8)
- **Import ordering**: managed by `reorder-python-imports` (Python 3.9+ style)
- **Type checking**: mypy with strict settings, though `tests/*`, `trino/client`, `trino/dbapi`, and `trino/sqlalchemy.*` have `ignore_errors = true`
- **No mocking libraries**: write mocks by hand instead of using `unittest.mock` or similar. The project uses `httpretty` for HTTP-level stubbing only.
- **Pre-commit hooks**: flake8, mypy, reorder-python-imports, trailing whitespace, EOF newlines, YAML syntax, case-conflict checks
