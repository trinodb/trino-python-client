# History of changes and releases

Release dates, binaries, and other information for the Trino Python client are
available in the [tags
list](https://github.com/trinodb/trino-python-client/tags), the
[README](https://github.com/trinodb/trino-python-client/blob/master/README.md)
and the [PyPI page](https://pypi.org/project/trino/).

## Trino Python client 0.315.0

* Add support for the `USE catalog.schema` statement.
* Add support for the `SET ROLE` statement.
* Make DBAPI errors PEP 249 compatible.

## Trino Python client 0.314.0

* Support for `time(p)` and `timestamp(p)` in SQLAlchemy.
* Fix failure when building `INSERT` statement from `WITH` clause in SQLAlchemy.

## Trino Python client 0.313.0

* Support `executemany` method in DBAPI
* Support keyring to securely cache the OAuth2 token
* Support client tags
* Improve type mapping

## Trino Python client 0.311.0

* Introduce `experimental_python_types` flag for decimal, date, time, timestamp
  with time zone and timestamp types.
* Add JWT token support via connection string in sqlalchemy.
* Get table comment from system catalog in sqlalchemy.

## Trino Python client 0.310.0

* Support SQLAlchemy in old server versions.
* Fix handling transaction requests in auto-commit mode.
* Add CertificateAuthentication class for cert based authentication.
* Add support for `extra_credential`.
* Fix `LIMIT` clause in SQLAlchemy.

## Trino Python client 0.309.0

* Add support for SQLAlchemy (requires Trino version >= 352).
* Fix multiple challenges handling in OAuth2.

## Trino Python client 0.308.0

* Add support full OAuth2 flow for authentication.
* Fix encode/decode error for session properties.
* Expose info_uri from query status in Cursor.
* Retry on 504 gateway timeout responses.
* Use HTTPS when port is 443.

## Trino Python client 0.306.0

* Add support for JWT Authentication.
* Remove support for Python 2.7.
* Remove support for Python 3.5.

## Older releases

Details for older releases are available in the [tags
list](https://github.com/trinodb/trino-python-client/tags), the code itself,
and the
[README](https://github.com/trinodb/trino-python-client/blob/master/README.md)
documentation.
