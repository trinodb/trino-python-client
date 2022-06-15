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
import datetime as dt
from typing import Iterator, List, Optional, Tuple, Type, Union

from sqlalchemy import util
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine

SQLType = Union[TypeEngine, Type[TypeEngine]]


class DOUBLE(sqltypes.Float):
    __visit_name__ = "DOUBLE"


class INTERVAL(sqltypes.NativeForEmulated, sqltypes._AbstractInterval):
    __visit_name__ = "INTERVAL"
    native = True

    def __init__(self, precision=None, fields=None, adapt_datatype: bool = False):
        """Construct an INTERVAL.

        :param precision: integer precision value
        :param fields: string fields specifier.  allows storage of fields
         to be limited, such as ``"YEAR"``, ``"MONTH"``, ``"SECOND"``,
         etc.
        :param adapt_datatype: allows conversion from data type value to column data type
        """
        self.precision = precision
        self.fields = fields
        self.adapt_datatype = adapt_datatype

    def adapt_value_to_datatype(self):
        return INTERVAL(
            precision=self.precision,
            fields=self.fields,
            adapt_datatype=True)

    @property
    def _type_affinity(self):
        return sqltypes.Interval

    def as_generic(self, allow_nulltype=False):
        return sqltypes.Interval(native=True, second_precision=self.precision)

    @property
    def python_type(self):
        return dt.timedelta

    def coerce_compared_value(self, op, value):
        return self


class MAP(TypeEngine):
    __visit_name__ = "MAP"

    def __init__(self, key_type: SQLType, value_type: SQLType):
        if isinstance(key_type, type):
            key_type = key_type()
        self.key_type: TypeEngine = key_type

        if isinstance(value_type, type):
            value_type = value_type()
        self.value_type: TypeEngine = value_type

    @property
    def python_type(self):
        return dict


class ROW(TypeEngine):
    __visit_name__ = "ROW"

    def __init__(self, attr_types: List[Tuple[Optional[str], SQLType]]):
        self.attr_types: List[Tuple[Optional[str], SQLType]] = []
        for attr_name, attr_type in attr_types:
            if isinstance(attr_type, type):
                attr_type = attr_type()
            self.attr_types.append((attr_name, attr_type))

    @property
    def python_type(self):
        return list


# https://trino.io/docs/current/language/types.html
_type_map = {
    # === Boolean ===
    "boolean": sqltypes.BOOLEAN,
    # === Integer ===
    "tinyint": sqltypes.SMALLINT,
    "smallint": sqltypes.SMALLINT,
    "int": sqltypes.INTEGER,
    "integer": sqltypes.INTEGER,
    "bigint": sqltypes.BIGINT,
    # === Floating-point ===
    "real": sqltypes.REAL,
    "double": DOUBLE,
    # === Fixed-precision ===
    "decimal": sqltypes.DECIMAL,
    # === String ===
    "varchar": sqltypes.VARCHAR,
    "char": sqltypes.CHAR,
    "varbinary": sqltypes.VARBINARY,
    "json": sqltypes.JSON,
    # === Date and time ===
    "date": sqltypes.DATE,
    "time": sqltypes.TIME,
    "timestamp": sqltypes.TIMESTAMP,
    "interval": INTERVAL,
    # === Structural ===
    # 'array': ARRAY,
    # 'map':   MAP
    # 'row':   ROW
    #
    # === Mixed ===
    # 'ipaddress': IPADDRESS
    # 'uuid': UUID,
    # 'hyperloglog': HYPERLOGLOG,
    # 'p4hyperloglog': P4HYPERLOGLOG,
    # 'qdigest': QDIGEST,
    # 'tdigest': TDIGEST,
}


def unquote(string: str, quote: str = '"', escape: str = "\\") -> str:
    """
    If string starts and ends with a quote, unquote it
    """
    if string.startswith(quote) and string.endswith(quote):
        string = string[1:-1]
        string = string.replace(f"{escape}{quote}", quote).replace(f"{escape}{escape}", escape)
    return string


def aware_split(
        string: str,
        delimiter: str = ",",
        maxsplit: int = -1,
        quote: str = '"',
        escaped_quote: str = r"\"",
        open_bracket: str = "(",
        close_bracket: str = ")",
) -> Iterator[str]:
    """
    A split function that is aware of quotes and brackets/parentheses.

    :param string: string to split
    :param delimiter: string defining where to split, usually a comma or space
    :param maxsplit: Maximum number of splits to do. -1 (default) means no limit.
    :param quote: string, either a single or a double quote
    :param escaped_quote: string representing an escaped quote
    :param open_bracket: string, either [, {, < or (
    :param close_bracket: string, either ], }, > or )
    """
    parens = 0
    quotes = False
    i = 0
    if maxsplit < -1:
        raise ValueError(f"maxsplit must be >= -1, got {maxsplit}")
    elif maxsplit == 0:
        yield string
        return
    for j, character in enumerate(string):
        complete = parens == 0 and not quotes
        if complete and character == delimiter:
            if maxsplit != -1:
                maxsplit -= 1
            yield string[i:j]
            i = j + len(delimiter)
            if maxsplit == 0:
                break
        elif character == open_bracket:
            parens += 1
        elif character == close_bracket:
            parens -= 1
        elif character == quote:
            if quotes and string[j - len(escaped_quote) + 1: j + 1] != escaped_quote:
                quotes = False
            elif not quotes:
                quotes = True
    yield string[i:]


def parse_sqltype(type_str: str) -> TypeEngine:
    type_str = type_str.strip().lower()
    match = re.match(r"^(?P<type>\w+)\s*(?:[\(|'](?P<precision>.*)[\)|'])?(?:[ ](?P<fields>.+))?", type_str)
    if not match:
        util.warn(f"Could not parse type name '{type_str}'")
        return sqltypes.NULLTYPE
    type_name = match.group("type")
    type_precision = match.group("precision")
    type_fields = match.group("fields")

    if type_name == "array":
        item_type = parse_sqltype(type_precision)
        if isinstance(item_type, sqltypes.ARRAY):
            # Multi-dimensions array is normalized in SQLAlchemy, e.g:
            # `ARRAY(ARRAY(INT))` in Trino SQL will become `ARRAY(INT(), dimensions=2)` in SQLAlchemy
            dimensions = (item_type.dimensions or 1) + 1
            return sqltypes.ARRAY(item_type.item_type, dimensions=dimensions)
        return sqltypes.ARRAY(item_type)
    elif type_name == "map":
        key_type_str, value_type_str = aware_split(type_precision)
        key_type = parse_sqltype(key_type_str)
        value_type = parse_sqltype(value_type_str)
        return MAP(key_type, value_type)
    elif type_name == "row":
        attr_types: List[Tuple[Optional[str], SQLType]] = []
        for attr in aware_split(type_precision):
            attr_name, attr_type_str = aware_split(attr.strip(), delimiter=" ", maxsplit=1)
            attr_name = unquote(attr_name)
            attr_type = parse_sqltype(attr_type_str)
            attr_types.append((attr_name, attr_type))
        return ROW(attr_types)

    if type_name not in _type_map:
        util.warn(f"Did not recognize type '{type_name}'")
        return sqltypes.NULLTYPE
    type_class = _type_map[type_name]
    type_args = [int(o.strip()) for o in type_precision.split(",")] if type_precision else []

    if type_name == "interval":
        if type_fields not in ("second", "minute", "hour", "day", "month", "year"):
            util.warn(f"Did not recognize field type '{type_fields}'")
            return sqltypes.NULLTYPE
        type_kwargs: Dict[str, Any] = dict(
            precision=int(type_precision),
            fields=type_fields
        )
        return type_class(**type_kwargs)

    if type_name in ("time", "timestamp"):
        type_kwargs = dict(timezone=type_str.endswith("with time zone"))
        # TODO: support parametric timestamps (https://github.com/trinodb/trino-python-client/issues/107)
        return type_class(**type_kwargs)
    return type_class(*type_args)
