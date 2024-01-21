from __future__ import annotations

import abc
import base64
import uuid
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

try:
    from zoneinfo import ZoneInfo
except ModuleNotFoundError:
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


class NoOpValueMapper(ValueMapper[Any]):
    def map(self, value) -> Optional[Any]:
        return value


class DecimalValueMapper(ValueMapper[Decimal]):
    def map(self, value) -> Optional[Decimal]:
        if value is None:
            return None
        return Decimal(value)


class DoubleValueMapper(ValueMapper[float]):
    def map(self, value) -> Optional[float]:
        if value is None:
            return None
        if value == 'Infinity':
            return float("inf")
        if value == '-Infinity':
            return float("-inf")
        if value == 'NaN':
            return float("nan")
        return float(value)


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


class TimeValueMapper(ValueMapper[time]):
    def __init__(self, precision):
        self.time_default_size = 8  # size of 'HH:MM:SS'
        self.precision = precision

    def map(self, value) -> Optional[time]:
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
    def map(self, value) -> Optional[time]:
        if value is None:
            return None
        whole_python_temporal_value = value[:self.time_default_size]
        remaining_fractional_seconds = value[self.time_default_size + 1:len(value) - 6]
        timezone_part = value[len(value) - 6:]
        return TimeWithTimeZone(
            time.fromisoformat(whole_python_temporal_value).replace(tzinfo=_create_tzinfo(timezone_part)),
            _fraction_to_decimal(remaining_fractional_seconds),
        ).round_to(self.precision).to_python_type()


class DateValueMapper(ValueMapper[date]):
    def map(self, value) -> Optional[date]:
        if value is None:
            return None
        return date.fromisoformat(value)


class TimestampValueMapper(ValueMapper[datetime]):
    def __init__(self, precision):
        self.datetime_default_size = 19  # size of 'YYYY-MM-DD HH:MM:SS' (the datetime string up to the seconds)
        self.precision = precision

    def map(self, value) -> Optional[datetime]:
        if value is None:
            return None
        whole_python_temporal_value = value[:self.datetime_default_size]
        remaining_fractional_seconds = value[self.datetime_default_size + 1:]
        return Timestamp(
            datetime.fromisoformat(whole_python_temporal_value),
            _fraction_to_decimal(remaining_fractional_seconds),
        ).round_to(self.precision).to_python_type()


class TimestampWithTimeZoneValueMapper(TimestampValueMapper):
    def map(self, value) -> Optional[datetime]:
        if value is None:
            return None
        datetime_with_fraction, timezone_part = value.rsplit(' ', 1)
        whole_python_temporal_value = datetime_with_fraction[:self.datetime_default_size]
        remaining_fractional_seconds = datetime_with_fraction[self.datetime_default_size + 1:]
        return TimestampWithTimeZone(
            datetime.fromisoformat(whole_python_temporal_value).replace(tzinfo=_create_tzinfo(timezone_part)),
            _fraction_to_decimal(remaining_fractional_seconds),
        ).round_to(self.precision).to_python_type()


class BinaryValueMapper(ValueMapper[bytes]):
    def map(self, value) -> Optional[bytes]:
        if value is None:
            return None
        return base64.b64decode(value.encode("utf8"))


class ArrayValueMapper(ValueMapper[List[Optional[Any]]]):
    def __init__(self, mapper: ValueMapper[Any]):
        self.mapper = mapper

    def map(self, values: List[Any]) -> Optional[List[Any]]:
        if values is None:
            return None
        return [self.mapper.map(value) for value in values]


class RowValueMapper(ValueMapper[Tuple[Optional[Any], ...]]):
    def __init__(self, mappers: List[ValueMapper[Any]], names: List[str], types: List[str]):
        self.mappers = mappers
        self.names = names
        self.types = types

    def map(self, values: List[Any]) -> Optional[Tuple[Optional[Any], ...]]:
        if values is None:
            return None
        return NamedRowTuple(
            list(self.mappers[index].map(value) for index, value in enumerate(values)),
            self.names,
            self.types
        )


class MapValueMapper(ValueMapper[Dict[Any, Optional[Any]]]):
    def __init__(self, key_mapper: ValueMapper[Any], value_mapper: ValueMapper[Any]):
        self.key_mapper = key_mapper
        self.value_mapper = value_mapper

    def map(self, values: Any) -> Optional[Dict[Any, Optional[Any]]]:
        if values is None:
            return None
        return {
            self.key_mapper.map(key): self.value_mapper.map(value) for key, value in values.items()
        }


class UuidValueMapper(ValueMapper[uuid.UUID]):
    def map(self, value: Any) -> Optional[uuid.UUID]:
        if value is None:
            return None
        return uuid.UUID(value)


class NoOpRowMapper:
    """
    No-op RowMapper which does not perform any transformation
    Used when legacy_primitive_types is False.
    """

    def map(self, rows):
        return rows


class RowMapperFactory:
    """
    Given the 'columns' result from Trino, generate a list of
    lambda functions (one for each column) which will process a data value
    and returns a RowMapper instance which will process rows of data
    """
    NO_OP_ROW_MAPPER = NoOpRowMapper()

    def create(self, columns, legacy_primitive_types):
        assert columns is not None

        if not legacy_primitive_types:
            return RowMapper([self._create_value_mapper(column['typeSignature']) for column in columns])
        return RowMapperFactory.NO_OP_ROW_MAPPER

    def _create_value_mapper(self, column) -> ValueMapper:
        col_type = column['rawType']

        if col_type == 'array':
            value_mapper = self._create_value_mapper(column['arguments'][0]['value'])
            return ArrayValueMapper(value_mapper)
        if col_type == 'row':
            mappers = []
            names = []
            types = []
            for arg in column['arguments']:
                mappers.append(self._create_value_mapper(arg['value']['typeSignature']))
                names.append(arg['value']['fieldName']['name'] if "fieldName" in arg['value'] else None)
                types.append(arg['value']['typeSignature']['rawType'])
            return RowValueMapper(mappers, names, types)
        if col_type == 'map':
            key_mapper = self._create_value_mapper(column['arguments'][0]['value'])
            value_mapper = self._create_value_mapper(column['arguments'][1]['value'])
            return MapValueMapper(key_mapper, value_mapper)
        if col_type == 'decimal':
            return DecimalValueMapper()
        if col_type in {'double', 'real'}:
            return DoubleValueMapper()
        if col_type == 'timestamp with time zone':
            return TimestampWithTimeZoneValueMapper(self._get_precision(column))
        if col_type == 'timestamp':
            return TimestampValueMapper(self._get_precision(column))
        if col_type == 'time with time zone':
            return TimeWithTimeZoneValueMapper(self._get_precision(column))
        if col_type == 'time':
            return TimeValueMapper(self._get_precision(column))
        if col_type == 'date':
            return DateValueMapper()
        if col_type == 'varbinary':
            return BinaryValueMapper()
        if col_type == 'uuid':
            return UuidValueMapper()
        return NoOpValueMapper()

    def _get_precision(self, column: Dict[str, Any]):
        args = column['arguments']
        if len(args) == 0:
            return 3
        return args[0]['value']


class RowMapper:
    """
    Maps a row of data given a list of mapping functions
    """
    def __init__(self, columns):
        self.columns = columns

    def map(self, rows):
        if len(self.columns) == 0:
            return rows
        return [self._map_row(row) for row in rows]

    def _map_row(self, row):
        return [self._map_value(value, self.columns[index]) for index, value in enumerate(row)]

    def _map_value(self, value, value_mapper: ValueMapper[T]) -> Optional[T]:
        try:
            return value_mapper.map(value)
        except ValueError as e:
            error_str = f"Could not convert '{value}' into the associated python type"
            raise trino.exceptions.TrinoDataError(error_str) from e
