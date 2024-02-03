from __future__ import annotations

import abc
import base64
import sys
import uuid
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo

import trino.exceptions
from trino.types import (
    POWERS_OF_TEN,
    NamedRowTuple,
    Time,
    Timestamp,
    TimestampWithTimeZone,
    TimeWithTimeZone,
)

T = TypeVar("T")


class ValueMapper(abc.ABC, Generic[T]):
    @abc.abstractmethod
    def map(self, value: Any) -> Optional[T]:
        pass


class BooleanValueMapper(ValueMapper[bool]):
    def map(self, value: Any) -> Optional[bool]:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if str(value).lower() == 'true':
            return True
        if str(value).lower() == 'false':
            return False
        raise ValueError(f"Server sent unexpected value {value} of type {type(value)} for boolean")


class IntegerValueMapper(ValueMapper[int]):
    def map(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        # int(3.1) == 3 but server won't send such values for integer types
        return int(value)


class DoubleValueMapper(ValueMapper[float]):
    def map(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        if value == 'Infinity':
            return float("inf")
        if value == '-Infinity':
            return float("-inf")
        if value == 'NaN':
            return float("nan")
        return float(value)


class DecimalValueMapper(ValueMapper[Decimal]):
    def map(self, value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        return Decimal(value)


class StringValueMapper(ValueMapper[str]):
    def map(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        return str(value)


class BinaryValueMapper(ValueMapper[bytes]):
    def map(self, value: Any) -> Optional[bytes]:
        if value is None:
            return None
        return base64.b64decode(value.encode("utf8"))


class DateValueMapper(ValueMapper[date]):
    def map(self, value: Any) -> Optional[date]:
        if value is None:
            return None
        return date.fromisoformat(value)


class TimeValueMapper(ValueMapper[time]):
    def __init__(self, precision: int):
        self.time_default_size = 8  # size of 'HH:MM:SS'
        self.precision = precision

    def map(self, value: Any) -> Optional[time]:
        if value is None:
            return None
        whole_python_temporal_value = value[:self.time_default_size]
        remaining_fractional_seconds = value[self.time_default_size + 1:]
        return Time(
            time.fromisoformat(whole_python_temporal_value),
            _fraction_to_decimal(remaining_fractional_seconds)
        ).round_to(self.precision).to_python_type()

    def _add_second(self, time_value: time) -> time:
        return (datetime.combine(datetime(1, 1, 1), time_value) + timedelta(seconds=1)).time()


class TimeWithTimeZoneValueMapper(TimeValueMapper):
    def map(self, value: Any) -> Optional[time]:
        if value is None:
            return None
        whole_python_temporal_value = value[:self.time_default_size]
        remaining_fractional_seconds = value[self.time_default_size + 1:len(value) - 6]
        timezone_part = value[len(value) - 6:]
        return TimeWithTimeZone(
            time.fromisoformat(whole_python_temporal_value).replace(tzinfo=_create_tzinfo(timezone_part)),
            _fraction_to_decimal(remaining_fractional_seconds),
        ).round_to(self.precision).to_python_type()


class TimestampValueMapper(ValueMapper[datetime]):
    def __init__(self, precision: int):
        self.datetime_default_size = 19  # size of 'YYYY-MM-DD HH:MM:SS' (the datetime string up to the seconds)
        self.precision = precision

    def map(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        whole_python_temporal_value = value[:self.datetime_default_size]
        remaining_fractional_seconds = value[self.datetime_default_size + 1:]
        return Timestamp(
            datetime.fromisoformat(whole_python_temporal_value),
            _fraction_to_decimal(remaining_fractional_seconds),
        ).round_to(self.precision).to_python_type()


class TimestampWithTimeZoneValueMapper(TimestampValueMapper):
    def map(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        datetime_with_fraction, timezone_part = value.rsplit(' ', 1)
        whole_python_temporal_value = datetime_with_fraction[:self.datetime_default_size]
        remaining_fractional_seconds = datetime_with_fraction[self.datetime_default_size + 1:]
        return TimestampWithTimeZone(
            datetime.fromisoformat(whole_python_temporal_value).replace(tzinfo=_create_tzinfo(timezone_part)),
            _fraction_to_decimal(remaining_fractional_seconds),
        ).round_to(self.precision).to_python_type()


def _create_tzinfo(timezone_str: str) -> tzinfo:
    if timezone_str.startswith("+") or timezone_str.startswith("-"):
        hours = timezone_str[1:3]
        minutes = timezone_str[4:6]
        if timezone_str.startswith("-"):
            return timezone(-timedelta(hours=int(hours), minutes=int(minutes)))
        return timezone(timedelta(hours=int(hours), minutes=int(minutes)))
    else:
        return ZoneInfo(timezone_str)


def _fraction_to_decimal(fractional_str: str) -> Decimal:
    return Decimal(fractional_str or 0) / POWERS_OF_TEN[len(fractional_str)]


class ArrayValueMapper(ValueMapper[List[Optional[Any]]]):
    def __init__(self, mapper: ValueMapper[Any]):
        self.mapper = mapper

    def map(self, value: Optional[List[Any]]) -> Optional[List[Any]]:
        if value is None:
            return None
        return [self.mapper.map(v) for v in value]


class MapValueMapper(ValueMapper[Dict[Any, Optional[Any]]]):
    def __init__(self, key_mapper: ValueMapper[Any], value_mapper: ValueMapper[Any]):
        self.key_mapper = key_mapper
        self.value_mapper = value_mapper

    def map(self, value: Any) -> Optional[Dict[Any, Optional[Any]]]:
        if value is None:
            return None
        return {
            self.key_mapper.map(k): self.value_mapper.map(v) for k, v in value.items()
        }


class RowValueMapper(ValueMapper[Tuple[Optional[Any], ...]]):
    def __init__(self, mappers: List[ValueMapper[Any]], names: List[Optional[str]], types: List[str]):
        self.mappers = mappers
        self.names = names
        self.types = types

    def map(self, value: Optional[List[Any]]) -> Optional[Tuple[Optional[Any], ...]]:
        if value is None:
            return None
        return NamedRowTuple(
            list(self.mappers[i].map(v) for i, v in enumerate(value)),
            self.names,
            self.types
        )


class UuidValueMapper(ValueMapper[uuid.UUID]):
    def map(self, value: Any) -> Optional[uuid.UUID]:
        if value is None:
            return None
        return uuid.UUID(value)


class NoOpValueMapper(ValueMapper[Any]):
    def map(self, value: Any) -> Optional[Any]:
        return value


class NoOpRowMapper:
    """
    No-op RowMapper which does not perform any transformation
    Used when legacy_primitive_types is False.
    """

    def map(self, rows: List[List[Any]]) -> List[List[Any]]:
        return rows


class RowMapperFactory:
    """
    Given the 'columns' result from Trino, generate a list of
    lambda functions (one for each column) which will process a data value
    and returns a RowMapper instance which will process rows of data
    """
    NO_OP_ROW_MAPPER = NoOpRowMapper()

    def create(self, columns: List[Any], legacy_primitive_types: bool) -> RowMapper | NoOpRowMapper:
        assert columns is not None

        if not legacy_primitive_types:
            return RowMapper([self._create_value_mapper(column['typeSignature']) for column in columns])
        return RowMapperFactory.NO_OP_ROW_MAPPER

    def _create_value_mapper(self, column: Dict[str, Any]) -> ValueMapper[Any]:
        col_type = column['rawType']

        # primitive types
        if col_type == 'boolean':
            return BooleanValueMapper()
        if col_type in {'tinyint', 'smallint', 'integer', 'bigint'}:
            return IntegerValueMapper()
        if col_type in {'double', 'real'}:
            return DoubleValueMapper()
        if col_type == 'decimal':
            return DecimalValueMapper()
        if col_type in {'varchar', 'char'}:
            return StringValueMapper()
        if col_type == 'varbinary':
            return BinaryValueMapper()
        if col_type == 'json':
            return StringValueMapper()
        if col_type == 'date':
            return DateValueMapper()
        if col_type == 'time':
            return TimeValueMapper(self._get_precision(column))
        if col_type == 'time with time zone':
            return TimeWithTimeZoneValueMapper(self._get_precision(column))
        if col_type == 'timestamp':
            return TimestampValueMapper(self._get_precision(column))
        if col_type == 'timestamp with time zone':
            return TimestampWithTimeZoneValueMapper(self._get_precision(column))

        # structural types
        if col_type == 'array':
            value_mapper = self._create_value_mapper(column['arguments'][0]['value'])
            return ArrayValueMapper(value_mapper)
        if col_type == 'map':
            key_mapper = self._create_value_mapper(column['arguments'][0]['value'])
            value_mapper = self._create_value_mapper(column['arguments'][1]['value'])
            return MapValueMapper(key_mapper, value_mapper)
        if col_type == 'row':
            mappers: List[ValueMapper[Any]] = []
            names: List[Optional[str]] = []
            types: List[str] = []
            for arg in column['arguments']:
                mappers.append(self._create_value_mapper(arg['value']['typeSignature']))
                names.append(arg['value']['fieldName']['name'] if "fieldName" in arg['value'] else None)
                types.append(arg['value']['typeSignature']['rawType'])
            return RowValueMapper(mappers, names, types)

        # others
        if col_type == 'uuid':
            return UuidValueMapper()
        return NoOpValueMapper()

    def _get_precision(self, column: Dict[str, Any]) -> int:
        args = column['arguments']
        if len(args) == 0:
            return 3
        return args[0]['value']


class RowMapper:
    """
    Maps a row of data given a list of mapping functions
    """
    def __init__(self, columns: List[ValueMapper[Any]]):
        self.columns = columns

    def map(self, rows: List[List[Any]]) -> List[List[Any]]:
        if len(self.columns) == 0:
            return rows
        return [self._map_row(row) for row in rows]

    def _map_row(self, row: List[Any]) -> List[Any]:
        return [self._map_value(value, self.columns[index]) for index, value in enumerate(row)]

    def _map_value(self, value: Any, value_mapper: ValueMapper[T]) -> Optional[T]:
        try:
            return value_mapper.map(value)
        except ValueError as e:
            error_str = f"Could not convert '{value}' into the associated python type"
            raise trino.exceptions.TrinoDataError(error_str) from e
