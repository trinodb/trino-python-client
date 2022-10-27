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
from sqlalchemy.sql import compiler
from sqlalchemy.sql.base import DialectKWArgs

# https://trino.io/docs/current/language/reserved.html
RESERVED_WORDS = {
    "alter",
    "and",
    "as",
    "between",
    "by",
    "case",
    "cast",
    "constraint",
    "create",
    "cross",
    "cube",
    "current_catalog",
    "current_date",
    "current_path",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "deallocate",
    "delete",
    "describe",
    "distinct",
    "drop",
    "else",
    "end",
    "escape",
    "except",
    "execute",
    "exists",
    "extract",
    "false",
    "for",
    "from",
    "full",
    "group",
    "grouping",
    "having",
    "in",
    "inner",
    "insert",
    "intersect",
    "into",
    "is",
    "join",
    "left",
    "like",
    "localtime",
    "localtimestamp",
    "natural",
    "normalize",
    "not",
    "null",
    "on",
    "or",
    "order",
    "outer",
    "prepare",
    "recursive",
    "right",
    "rollup",
    "select",
    "skip",
    "table",
    "then",
    "true",
    "uescape",
    "union",
    "unnest",
    "using",
    "values",
    "when",
    "where",
    "with",
}


class TrinoSQLCompiler(compiler.SQLCompiler):
    def limit_clause(self, select, **kw):
        """
        Trino support only OFFSET...LIMIT but not LIMIT...OFFSET syntax.
        """
        text = ""
        if select._offset_clause is not None:
            text += "\nOFFSET " + self.process(select._offset_clause, **kw)
        if select._limit_clause is not None:
            text += "\nLIMIT " + self.process(select._limit_clause, **kw)
        return text

    def visit_table(self, table, asfrom=False, iscrud=False, ashint=False,
                    fromhints=None, use_schema=True, **kwargs):
        sql = super(TrinoSQLCompiler, self).visit_table(
            table, asfrom, iscrud, ashint, fromhints, use_schema, **kwargs
        )
        return self.add_catalog(sql, table)

    @staticmethod
    def add_catalog(sql, table):
        if table is None or not isinstance(table, DialectKWArgs):
            return sql

        if (
                'trino' not in table.dialect_options
                or 'catalog' not in table.dialect_options['trino']
        ):
            return sql

        catalog = table.dialect_options['trino']['catalog']
        sql = f'"{catalog}".{sql}'
        return sql


class TrinoDDLCompiler(compiler.DDLCompiler):
    pass


class TrinoTypeCompiler(compiler.GenericTypeCompiler):
    def visit_FLOAT(self, type_, **kw):
        precision = type_.precision or 32
        if 0 <= precision <= 32:
            return self.visit_REAL(type_, **kw)
        elif 32 < precision <= 64:
            return self.visit_DOUBLE(type_, **kw)
        else:
            raise ValueError(f"type.precision must be in range [0, 64], got {type_.precision}")

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kw):
        return self.visit_DECIMAL(type_, **kw)

    def visit_NCHAR(self, type_, **kw):
        return self.visit_CHAR(type_, **kw)

    def visit_NVARCHAR(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_TEXT(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_BINARY(self, type_, **kw):
        return self.visit_VARBINARY(type_, **kw)

    def visit_CLOB(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_NCLOB(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_BLOB(self, type_, **kw):
        return self.visit_VARBINARY(type_, **kw)

    def visit_DATETIME(self, type_, **kw):
        return self.visit_TIMESTAMP(type_, **kw)

    def visit_TIMESTAMP(self, type_, **kw):
        datatype = "TIMESTAMP"
        precision = getattr(type_, "precision", None)
        if precision not in range(0, 13) and precision is not None:
            raise ValueError(f"invalid precision={precision}, it must be from range 0-12")
        if precision is not None:
            datatype += f"({precision})"
        if getattr(type_, "timezone", False):
            datatype += " WITH TIME ZONE"

        return datatype

    def visit_TIME(self, type_, **kw):
        datatype = "TIME"
        precision = getattr(type_, "precision", None)
        if precision not in range(0, 13) and precision is not None:
            raise ValueError(f"invalid precision={precision}, it must be from range 0-12")
        if precision is not None:
            datatype += f"({precision})"
        if getattr(type_, "timezone", False):
            datatype += " WITH TIME ZONE"
        return datatype

    def visit_JSON(self, type_, **kw):
        return 'JSON'


class TrinoIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

    def format_table(self, table, use_schema=True, name=None):
        result = super(TrinoIdentifierPreparer, self).format_table(table, use_schema, name)
        return TrinoSQLCompiler.add_catalog(result, table)
