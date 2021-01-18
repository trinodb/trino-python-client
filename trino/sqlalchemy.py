# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import re

from sqlalchemy.engine import default
from sqlalchemy import exc
from sqlalchemy.sql import compiler

import trino
import trino.logging


logger = trino.logging.get_logger(__name__)


class UniversalSet(object):
    """Helper class to TrinoIdentifierPreparer"""
    def __contains__(self, item):
        return True


class TrinoIdentifierPreparer(compiler.IdentifierPreparer):
    """Quote all identifiers

       TODO: Update with Trino SQL syntax
    """
    reserved_words = UniversalSet()


class TrinoCompiler(compiler.SQLCompiler):
    """Compile SQL commands"""
    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))


class TrinoTypeCompiler(compiler.GenericTypeCompiler):
    """Compile types for SQL commands"""
    def visit_CLOB(self, type_, **kw):
        raise ValueError('Trino does not support the CLOB column type.')

    def visit_NCLOB(self, type_, **kw):
        raise ValueError('Trino does not support the NCLOB column type.')

    def visit_DATETIME(self, type_, **kw):
        raise ValueError('Trino does not support the DATETIME column type.')

    def visit_TEXT(self, type_, **kw):
        if type_.length:
            return 'VARCHAR({:d})'.format(type_.length)
        else:
            return 'VARCHAR'


class TrinoDialect(default.DefaultDialect):
    name = 'trino'
    driver = 'rest'
    preparer = TrinoIdentifierPreparer
    statement_compiler = TrinoCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    type_compiler = TrinoTypeCompiler

    @staticmethod
    def dbapi():
        return trino.dbapi

    @staticmethod
    def create_connect_args(url):
        """Construct args for Connection from SQLAlchemy connection string"""
        kwargs = {
            'user': url.username,
            'host': url.host,
            'port': url.port or 8080,
        }

        if url.query.get('ssl') == 'true':
            kwargs['http_scheme'] = trino.constants.HTTPS

        if url.password is not None:
            kwargs['auth'] = trino.auth.BasicAuthentication(url.username, url.password)

        if url.database is not None:
            database_parts = url.database.split('/')
            if len(database_parts) == 1:
                kwargs['catalog'] = database_parts[0]
            elif len(database_parts) == 2:
                kwargs['catalog'] = database_parts[0]
                kwargs['schema'] = database_parts[1]
            else:
                raise ValueError('Unexpected database format {}'.format(url.database))

        return [], kwargs

    def _get_table_columns(self, connection, table_name, schema):
        """Helper to determine whether table with schema exists"""
        full_table = self.identifier_preparer.quote_identifier(table_name)
        if schema:
            full_table = self.identifier_preparer.quote_identifier(schema) + '.' + full_table
        try:
            return connection.execute('SHOW COLUMNS FROM {}'.format(full_table))
        except (trino.DatabaseError, exc.DatabaseError) as e:
            msg = (
                e.args[0].get('message') if e.args and isinstance(e.args[0], dict)
                else e.args[0] if e.args and isinstance(e.args[0], str)
                else None
            )
            regex = r'Table\ \'.*{}\'\ does\ not\ exist'.format(re.escape(table_name))
            if msg and re.search(regex, msg):
                raise exc.NoSuchTableError(table_name)
            else:
                raise

    def has_table(self, connection, table_name, schema=None):
        """Determine whether table with schema exists"""
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Return columns which are partition keys"""
        rows = self._get_table_columns(connection, table_name, schema)
        col_names = []
        for row in rows:
            part_key = 'Partition Key'
            # Trino puts this information in one of 3 places depending on version
            # - a boolean column named "Partition Key"
            # - a string in the "Comment" column
            # - a string in the "Extra" column
            is_partition_key = (
                (part_key in row and row[part_key])
                or row['Comment'].startswith(part_key)
                or ('Extra' in row and 'partition key' in row['Extra'])
            )
            if is_partition_key:
                col_names.append(row['Column'])
        if col_names:
            return [{'name': 'partition', 'column_names': col_names, 'unique': False}]
        else:
            return []

    def do_rollback(self, dbapi_connection):
        # TODO: This feature is currently not working due to the fact that
        # SQLAlchemy calls `do_rollback` prematurely. Call `dbapi_connection.rollback`
        # once fix has been determined.
        pass
