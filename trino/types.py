from __future__ import annotations

import abc
from datetime import datetime, time, timedelta
from decimal import Decimal
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar, Union, cast

from dateutil import tz

PythonTemporalType = TypeVar("PythonTemporalType", bound=Union[time, datetime])
POWERS_OF_TEN: Dict[int, Decimal] = {i: Decimal(10**i) for i in range(0, 13)}
MAX_PYTHON_TEMPORAL_PRECISION_POWER = 6
MAX_PYTHON_TEMPORAL_PRECISION = POWERS_OF_TEN[MAX_PYTHON_TEMPORAL_PRECISION_POWER]


class TemporalType(Generic[PythonTemporalType], metaclass=abc.ABCMeta):
    def __init__(self, whole_python_temporal_value: PythonTemporalType, remaining_fractional_seconds: Decimal):
        self._whole_python_temporal_value = whole_python_temporal_value
        self._remaining_fractional_seconds = remaining_fractional_seconds

    @abc.abstractmethod
    def new_instance(self, value: PythonTemporalType, fraction: Decimal) -> TemporalType[PythonTemporalType]:
        pass

    @abc.abstractmethod
    def to_python_type(self) -> PythonTemporalType:
        pass

    def round_to(self, precision: int) -> TemporalType[PythonTemporalType]:
        """
            Python datetime and time only support up to microsecond precision
            In case the supplied value exceeds the specified precision,
            the value needs to be rounded.
        """
        precision = min(precision, MAX_PYTHON_TEMPORAL_PRECISION_POWER)
        remaining_fractional_seconds = self._remaining_fractional_seconds
        # exponent can return `n`, `N`, `F` too if the value is a NaN for example
        digits = abs(remaining_fractional_seconds.as_tuple().exponent)  # type: ignore
        if digits > precision:
            rounding_factor = POWERS_OF_TEN[precision]
            rounded = remaining_fractional_seconds.quantize(Decimal(1 / rounding_factor))
            if rounded == rounding_factor:
                return self.new_instance(
                    self.normalize(self.add_time_delta(timedelta(seconds=1))),
                    Decimal(0)
                )
            return self.new_instance(self._whole_python_temporal_value, rounded)
        return self

    @abc.abstractmethod
    def add_time_delta(self, time_delta: timedelta) -> PythonTemporalType:
        """
            This method shall be overriden to implement fraction arithmetics.
        """
        pass

    def normalize(self, value: PythonTemporalType) -> PythonTemporalType:
        """
            If `add_time_delta` results in value crossing DST boundaries, this method should
            return a normalized version of the value to account for it.
        """
        return value


class Time(TemporalType[time]):
    def new_instance(self, value: time, fraction: Decimal) -> TemporalType[time]:
        return Time(value, fraction)

    def to_python_type(self) -> time:
        if self._remaining_fractional_seconds > 0:
            time_delta = timedelta(microseconds=int(self._remaining_fractional_seconds * MAX_PYTHON_TEMPORAL_PRECISION))
            return self.add_time_delta(time_delta)
        return self._whole_python_temporal_value

    def add_time_delta(self, time_delta: timedelta) -> time:
        time_delta_added = datetime.combine(datetime(1, 1, 1), self._whole_python_temporal_value) + time_delta
        return time_delta_added.time().replace(tzinfo=self._whole_python_temporal_value.tzinfo)


class TimeWithTimeZone(Time, TemporalType[time]):
    def new_instance(self, value: time, fraction: Decimal) -> TemporalType[time]:
        return TimeWithTimeZone(value, fraction)


class Timestamp(TemporalType[datetime]):
    def new_instance(self, value: datetime, fraction: Decimal) -> Timestamp:
        return Timestamp(value, fraction)

    def to_python_type(self) -> datetime:
        if self._remaining_fractional_seconds > 0:
            time_delta = timedelta(microseconds=int(self._remaining_fractional_seconds * MAX_PYTHON_TEMPORAL_PRECISION))
            return self.add_time_delta(time_delta)
        return self._whole_python_temporal_value

    def add_time_delta(self, time_delta: timedelta) -> datetime:
        return self._whole_python_temporal_value + time_delta


class TimestampWithTimeZone(Timestamp, TemporalType[datetime]):
    def new_instance(self, value: datetime, fraction: Decimal) -> TimestampWithTimeZone:
        return TimestampWithTimeZone(value, fraction)

    def normalize(self, value: datetime) -> datetime:
        if tz.datetime_ambiguous(value):
            # This appears to be dead code since tzinfo doesn't actually have a `normalize` method.
            # TODO: Fix this or remove. (https://github.com/trinodb/trino-python-client/issues/449)
            return self._whole_python_temporal_value.tzinfo.normalize(value)    # type: ignore
        return value


class NamedRowTuple(Tuple[Any, ...]):
    """Custom tuple class as namedtuple doesn't support missing or duplicate names"""
    def __new__(cls, values: List[Any], names: List[str], types: List[str]) -> NamedRowTuple:
        return cast(NamedRowTuple, super().__new__(cls, values))

    def __init__(self, values: List[Any], names: List[Optional[str]], types: List[str]):
        self._names = names
        # With names and types users can retrieve the name and Trino data type of a row
        self.__annotations__ = dict()
        self.__annotations__["names"] = names
        self.__annotations__["types"] = types
        elements: List[Any] = []
        for name, value in zip(names, values):
            if name is not None and names.count(name) == 1:
                setattr(self, name, value)
                elements.append(f"{name}: {repr(value)}")
            else:
                elements.append(repr(value))
        self._repr = "(" + ", ".join(elements) + ")"

    def __getattr__(self, name: str) -> Any:
        if self._names.count(name):
            raise ValueError("Ambiguous row field reference: " + name)

    def __repr__(self) -> str:
        return self._repr
