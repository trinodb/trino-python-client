# History of changes and releases

Release dates, binaries, and other information for the Trino Python client are
available in the [tags
list](https://github.com/trinodb/trino-python-client/tags), the
[README](https://github.com/trinodb/trino-python-client/blob/master/README.md)
and the [PyPI page](https://pypi.org/project/trino/).

## Release 0.333.0

* Improve handling of query results containing `null` values for
  the error field.
  ([#512](https://github.com/trinodb/trino-python-client/pull/512))
* Fix accessing spooled segments on S3 when authentication to the
  Trino cluster is used.
  ([#525](https://github.com/trinodb/trino-python-client/pull/525))
* Simplify `segment` cursor API to make it easier to iterate over spooled
  segments.
  ([#525](https://github.com/trinodb/trino-python-client/pull/525))

## Release 0.332.0

* Add support for spooling protocol.
  ([#509](https://github.com/trinodb/trino-python-client/pull/509))

## Release 0.331.0

* Add support for Python 3.13 and drop support for Python 3.8 which is
  end-of-life.
  ([#489](https://github.com/trinodb/trino-python-client/issues/489))
* Add support for Trino `INTERVAL YEAR TO MONTH` type by mapping to Python
  `relativedelta`.
  ([#475](https://github.com/trinodb/trino-python-client/issues/475))
* Add support for Trino `INTERVAL DAY TO SECOND` type by mapping to Python
  `timedelta`.
  ([#475](https://github.com/trinodb/trino-python-client/issues/475))
* Custom types are now pickle-able which allows frameworks like
  [Streamlit](https://github.com/streamlit/streamlit) to work.
  ([#490](https://github.com/trinodb/trino-python-client/issues/490))
* Fix OAuth2 authentication to not fail if the redirect server or token server
  URIs contain parameters.
  ([#495](https://github.com/trinodb/trino-python-client/issues/495))

## Release 0.330.0

* Add support for `try_cast` to SQLAlchemy dialect.
  ([#473](https://github.com/trinodb/trino-python-client/issues/473))
* Add retries on HTTP 429 error from Trino or a gateway/proxy using the
  `Retry-After` HTTP header in the response.
  ([#457](https://github.com/trinodb/trino-python-client/issues/457))
* Fix token caching for long tokens on Windows.
  ([#477](https://github.com/trinodb/trino-python-client/issues/477))
* Fix parsing of multi-valued `WWW-Authenticate` HTTP headers.
  ([#484](https://github.com/trinodb/trino-python-client/issues/484))
* Fix SQLAlchemy `get_indexes` implementation to return partition column names
  for Hive connector instead of returning column names of the
  `<table>$partitions` table.
  ([#476](https://github.com/trinodb/trino-python-client/issues/476))
* Fix parsing of `cert` and `key` from SQLAlchemy connection URI.
  ([#478](https://github.com/trinodb/trino-python-client/issues/478))

## Release 0.329.0

* Add support for `SET SESSION AUTHORIZATION`.
  ([#349](https://github.com/trinodb/trino-python-client/issues/349))
* Add support for GSSAPI based Kerberos authentication. Use
  `auth=GSSAPIAuthentication(...)` to use it.
  ([#454](https://github.com/trinodb/trino-python-client/issues/454))
* Add support to SQLAlchemy dialect for `ROW` and `ARRAY` DDL generation.
  ([#458](https://github.com/trinodb/trino-python-client/issues/458))
* Fix possible OAuth2 authentication failures when identity providers return
  multiple challenges.
  ([#444](https://github.com/trinodb/trino-python-client/issues/444))

## Release 0.328.0

* Add support for Python 3.12 and drop support for Python 3.7 and PyPy 3.8
  which are end-of-life.
  ([#438](https://github.com/trinodb/trino-python-client/issues/438))
* Add support for `JSON` data type to SQLAlchemy dialect.
  ([#387](https://github.com/trinodb/trino-python-client/issues/387))
* Add support for `IGNORE NULLS` in window functions when using SQLAlchemy.
  ([#429](https://github.com/trinodb/trino-python-client/issues/429))
* Add support for `MAP` type to SQLAlchemy dialect.
  ([#397](https://github.com/trinodb/trino-python-client/issues/397))
* Add support for per-user OAuth2 token cache if the `user` is provided when
  creating the connection. If `user` is not provided the previous behaviour of
  a per-host token cache is used.
  ([#430](https://github.com/trinodb/trino-python-client/issues/430))
* Add support to configure log level for all modules via the root logger. The
  root logger is named `trino`.
  ([#434](https://github.com/trinodb/trino-python-client/issues/434))
* Allow using `keyring.backends.null.Keyring` to disable keyring when using
  OAuth2.
  ([#431](https://github.com/trinodb/trino-python-client/issues/431))
* Fix `fetchall` returning already returned rows if called after `fetchone` or
  `fetchmany`.
  ([#414](https://github.com/trinodb/trino-python-client/issues/414))
* Fix possible failures when retrieving query results if the server returns
  empty data.
  ([#443](https://github.com/trinodb/trino-python-client/issues/443))
* Fix error when closing a cursor without executing a query.
  ([#428](https://github.com/trinodb/trino-python-client/issues/428))
* Fix `get_indexes` in SQLAlchemy to not fail when called for non-Hive tables.
  ([#426](https://github.com/trinodb/trino-python-client/issues/426))
* Fix reading and writing `MAP` types with boolean or integer keys.
  ([#441](https://github.com/trinodb/trino-python-client/issues/441))
* Fix incompatibility when using trino-python-client with
  [Ray](https://github.com/ray-project/ray).
  ([#420](https://github.com/trinodb/trino-python-client/issues/420))

## Release 0.327.0

* Implement `get_catalog_names` for SQLAlchemy to get the list of catalogs
  present on the cluster.
  ([#401](https://github.com/trinodb/trino-python-client/issues/401))
* Use `OAuth2Authentication` if `externalAuthentication` parameter is set on
  the connection url when using SQLAlchemy.
  ([#343](https://github.com/trinodb/trino-python-client/issues/343))
* Set the `User-Agent` HTTP header in all requests to `Trino Python
  Client/<client_version>`.
  ([#411](https://github.com/trinodb/trino-python-client/issues/411))
* Raise `TrinoConnectionError` for all connection related errors.
  ([#364](https://github.com/trinodb/trino-python-client/issues/364))

## Release 0.326.0

* Fix SQLAlchemy URL failing to parse if `legacy_prepared_statements` parameter
  was specified.
  ([#380](https://github.com/trinodb/trino-python-client/issues/380))
* Fix more than required `EXECUTE IMMEDIATE` queries being executed when
  `legacy_prepared_statements` parameter wasn't explicitly set.
  ([#380](https://github.com/trinodb/trino-python-client/issues/380))

## Release 0.325.0

* Queries with parameters now uses `EXECUTE IMMEDIATE` instead of `PREPARE`,
  `EXECUTE` and `DEALLOCATE` if the server supports `EXECUTE IMMEDIATE`. If
  `EXECUTE IMMEDIATE` is not support the client falls back to using `PREPARE`,
  `EXECUTE` and `DEALLOCATE`. If you want to always use `PREPARE`, `EXECUTE`
  and `DEALLOCATE` set the kwarg `legacy_prepared_statements` to `True`.
  ([#375](https://github.com/trinodb/trino-python-client/issues/375))

## Release 0.324.0

* Reduce the number of times `SELECT version()` query is sent to Trino when
  using SQLAlchemy.
  ([#371](https://github.com/trinodb/trino-python-client/issues/371))
* Fix `TIMESTAMP WITH TIME ZONE` being mapped to incorrect zones.
  ([#366](https://github.com/trinodb/trino-python-client/issues/366))

## Release 0.323.0

* Fix handling of `Decimal` params which use scientific notation.
  ([#347](https://github.com/trinodb/trino-python-client/issues/347))
* Map Trino `UUID` type to Python `uuid.UUID`.
  ([#354](https://github.com/trinodb/trino-python-client/issues/354))
* Map `sqlalchemy.Uuid` type to Trino `UUID`.
  ([#359](https://github.com/trinodb/trino-python-client/issues/359))
* Support using timezone aware `datetime.time` params in prepared statements.
  ([#360](https://github.com/trinodb/trino-python-client/issues/360))
* Allow accessing `ROW` type fields using their names in addition to their
  indices.
  ([#338](https://github.com/trinodb/trino-python-client/issues/338))
* Interpret `roles` without catalog name as system roles for convenience.
  ([#341](https://github.com/trinodb/trino-python-client/issues/341))

## Release 0.322.0

* Return `rowcount` property on the DB-API `Cursor` when available.
  ([#325](https://github.com/trinodb/trino-python-client/issues/325))
* Provide a read-only property `query` on the DB-API `Cursor` which contains
  the query text submitted through the cursor when available.
  ([#323](https://github.com/trinodb/trino-python-client/issues/323))
* Automatically determine `http_scheme`, `host` and `port` from `host` if it's
  a valid URL.
  ([#340](https://github.com/trinodb/trino-python-client/issues/340))
* Fix query cancellation to follow the protocol. Before this change cancelling
  a query could lead to errors from the Trino server.
  ([#328](https://github.com/trinodb/trino-python-client/issues/328))

## Release 0.321.0

* Add support for SQLAlchemy 2.0.
  ([#307](https://github.com/trinodb/trino-python-client/issues/307))
* Add support for `varbinary` query parameters.
  ([#299](https://github.com/trinodb/trino-python-client/issues/299))
* Add `Cursor.describe` method to return some metadata about the results of a
  query.
  ([#302](https://github.com/trinodb/trino-python-client/issues/302))
* Add `internal_size`, `precision` and `scale` in `Cursor.description`.
  ([#315](https://github.com/trinodb/trino-python-client/issues/315))
* Add support for chaining methods on `Cursor.execute`.
  ([#279](https://github.com/trinodb/trino-python-client/issues/279))
* Fix bug where passing `roles` to `Connection` did not enable the provided
  roles.
  ([#311](https://github.com/trinodb/trino-python-client/issues/311))

### Breaking Changes

* The client now maps query results to Python types by default. In older
  versions this could be enabled explicitly by passing
  `experimental_python_types=True` to the `trino.dbapi.connect` method. To
  restore the old behaviour of mapping results to primitive types you can pass
  `legacy_primitive_types=True` to the `trino.dbapi.connect` method. See the
  [documentation](https://github.com/trinodb/trino-python-client#legacy-primitive-types)
  to learn more.
  ([#305](https://github.com/trinodb/trino-python-client/issues/305))
* Add support for setting the session timezone. When not set explicitly it
  defaults to the client side local timezone. This changes the behaviour of the
  client in backward-incompatible way. To preserve the behaviour from client
  versions older than 0.321.0 you can explicitly pass `timezone='UTC'` to
  `trino.dbapi.connect` when creating the connection.
  ([#27](https://github.com/trinodb/trino-python-client/issues/27))
* Add support for variable precision datetime types. This change makes temporal
  types contain the correct precision as computed by Trino instead of being
  always limited to millisecond precision.
  ([#300](https://github.com/trinodb/trino-python-client/issues/300))

## Release 0.320.0

* Fix handling of expired access tokens when using OAuth 2 authentication.
  ([#284](https://github.com/trinodb/trino-python-client/issues/284))
* Support `None` values in array, map and row types when
  `experimental_python_types` is enabled.
  ([#269](https://github.com/trinodb/trino-python-client/issues/269))
* Expose query id of most recently executed query on a cursor as
  `Cursor.query_id`.
  ([#295](https://github.com/trinodb/trino-python-client/issues/295))

## Release 0.319.0

* Improve the performance of `get_view_names` in SQLAlchemy.
  ([#267](https://github.com/trinodb/trino-python-client/issues/267))
* Fix possible `ValueError` when client receives empty HTTP headers.
  ([#262](https://github.com/trinodb/trino-python-client/issues/262))
* Fix a compatibility issue with `_rfc_1738_quote` in SQLAlchemy v1.4.42.
  ([#273](https://github.com/trinodb/trino-python-client/issues/273))
* Return only tables in SQLAlchemy `get_table_names`. Previously, it
  also contained views.
  ([#266](https://github.com/trinodb/trino-python-client/issues/266))

## Release 0.318.0

* Fix the SQLAlchemy dialect to be compatible with SQLAlchemy 1.3.x versions.
  ([#250](https://github.com/trinodb/trino-python-client/issues/250))
* Fix possible `KeyError` when using prepared statements.
  ([#249](https://github.com/trinodb/trino-python-client/issues/249))
* Fix failure when calling `get_table_comment` using SQLAlchemy.
  ([#253](https://github.com/trinodb/trino-python-client/issues/253))

## Release 0.317.0

* Add support for creating tables containing `JSON` columns and reading and
  writing to them with SQLAlchemy.
  ([#194](https://github.com/trinodb/trino-python-client/issues/194))
* Add support for setting roles by passing a dictionary of catalog name and
  role as the `roles` keyword argument to `trino.dbapi.connect`.
  ([#230](https://github.com/trinodb/trino-python-client/issues/230))
* Add support for setting roles by adding the `roles` URL query parameter in
  SQLAlchemy connections to a JSON object with keys as the catalog name and
  values as the role name.
  ([#230](https://github.com/trinodb/trino-python-client/issues/230))
* Add a function `trino.sqlalchemy.URL` to generate SQLAlchemy URLs which
  properly handles escaping and encoding values where needed.
  ([#235](https://github.com/trinodb/trino-python-client/issues/235))
* Fix query failures not being propagated to the client when using `fetchone`.
  ([#95](https://github.com/trinodb/trino-python-client/issues/95))
* Fix queries returning a single row from sometimes appearing as failed on the
  server. ([#220](https://github.com/trinodb/trino-python-client/issues/220))
* Fix query failures when using SQLAlchemy `TableClause` by not performing
  catalog lookup.
  ([#237](https://github.com/trinodb/trino-python-client/issues/237))
* Fix errors when using prepared statements with Trino versions greater than or
  equal to 398.
  ([#242](https://github.com/trinodb/trino-python-client/issues/242))

### Breaking Changes

* Block the `execute` method of the cursor until at least one row is received.
  Users no longer need to call `fetchone` or `fetchall` to ensure query starts
  executing on the Trino server. Note that results still need to be consumed by
  calling `fetchone` or `fetchall` to ensure that a query isn't considered idle
  and terminated on the server.
  ([#232](https://github.com/trinodb/trino-python-client/issues/232))

## Release 0.316.0

* Add support for SQLAlchemy queries to access multiple catalogs by specifying
  a `trino_catalog` argument to SQLAlchemy `Table` objects.
  ([#186](https://github.com/trinodb/trino-python-client/issues/186))
* Improve performance when a cursor with `experimental_python_types` is used.
  ([#206](https://github.com/trinodb/trino-python-client/issues/206))
* Fix incorrect results for `get_table_comment` in SQLAlchemy when two tables
  with the same name and schema exist in different catalogs.
  ([#217](https://github.com/trinodb/trino-python-client/issues/217))
* Remove spurious logging of HTTP responses when a query is cancelled.
  ([#216](https://github.com/trinodb/trino-python-client/issues/216))

## Older releases

Details for older releases are available in the [tags
list](https://github.com/trinodb/trino-python-client/tags), the code itself,
and the
[README](https://github.com/trinodb/trino-python-client/blob/master/README.md)
documentation.
