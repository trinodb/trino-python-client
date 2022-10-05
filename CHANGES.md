# History of changes and releases

Release dates, binaries, and other information for the Trino Python client are
available in the [tags
list](https://github.com/trinodb/trino-python-client/tags), the
[README](https://github.com/trinodb/trino-python-client/blob/master/README.md)
and the [PyPI page](https://pypi.org/project/trino/).

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
