# History of changes and releases

Release dates, binaries, and other information for the Trino Python client are
available in the [tags
list](https://github.com/trinodb/trino-python-client/tags), the
[README](https://github.com/trinodb/trino-python-client/blob/master/README.md)
and the [PyPI page](https://pypi.org/project/trino/).

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
* Allow accessing `ROW` type fields using their names in addition to their indices.
  ([#338](https://github.com/trinodb/trino-python-client/issues/338))
* Interpret `roles` without catalog name as system roles for convenience.
  ([#341](https://github.com/trinodb/trino-python-client/issues/341))

## Release 0.322.0

* Return `rowcount` property on the DB-API `Cursor` when available.
  ([#325](https://github.com/trinodb/trino-python-client/issues/325))
* Provide a read-only property `query` on the DB-API `Cursor` which contains
  the query text submitted through the cursor when available.
  ([#323](https://github.com/trinodb/trino-python-client/issues/323))
* Automatically determine `http_scheme`, `host` and `port` from `host` if it's a
  valid URL.
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
* Fix bug where passing `roles` to `Connection` did not enable the provided roles.
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
* Support `None` values in array, map and row types when `experimental_python_types` is enabled.
  ([#269](https://github.com/trinodb/trino-python-client/issues/269))
* Expose query id of most recently executed query on a cursor as `Cursor.query_id`.
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
