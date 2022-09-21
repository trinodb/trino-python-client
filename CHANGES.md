# History of changes and releases

Release dates, binaries, and other information for the Trino Python client are
available in the [tags
list](https://github.com/trinodb/trino-python-client/tags), the
[README](https://github.com/trinodb/trino-python-client/blob/master/README.md)
and the [PyPI page](https://pypi.org/project/trino/).

## Trino Python client 0.316.0

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
