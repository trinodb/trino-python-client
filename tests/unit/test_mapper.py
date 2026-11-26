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
from datetime import timedelta
from decimal import Decimal

import pytest

from trino import constants
from trino.exceptions import TrinoDataError
from trino.mapper import DecimalValueMapper
from trino.mapper import IntervalDayToSecondMapper
from trino.mapper import RowMapperFactory


def _column(raw_type, arguments=None):
    """Build a minimal typeSignature dict as produced by Trino."""
    return {"rawType": raw_type, "arguments": arguments or []}


class TestNumberClientCapability:
    def test_number_capability_constant(self):
        assert constants.CLIENT_CAPABILITY_NUMBER == "NUMBER"

    def test_number_is_advertised_in_client_capabilities(self):
        capabilities = constants.CLIENT_CAPABILITIES.split(",")
        assert constants.CLIENT_CAPABILITY_NUMBER in capabilities


class TestRowMapperFactoryNumber:
    @pytest.mark.parametrize("raw_type", ["number", "decimal"])
    def test_number_and_decimal_use_decimal_mapper(self, raw_type):
        mapper = RowMapperFactory()._create_value_mapper(_column(raw_type))
        assert isinstance(mapper, DecimalValueMapper)

    def test_number_column_maps_value_to_decimal(self):
        mapper = RowMapperFactory()._create_value_mapper(_column("number"))
        result = mapper.map("10.3")
        assert isinstance(result, Decimal)
        assert result == Decimal("10.3")

    def test_number_column_maps_negative_literal(self):
        mapper = RowMapperFactory()._create_value_mapper(_column("number"))
        result = mapper.map("-10.3")
        assert isinstance(result, Decimal)
        assert result == Decimal("-10.3")

    def test_number_column_maps_positive_exponent(self):
        mapper = RowMapperFactory()._create_value_mapper(_column("number"))
        assert mapper.map("1.5E3") == Decimal("1500")

    def test_number_column_maps_negative_exponent(self):
        mapper = RowMapperFactory()._create_value_mapper(_column("number"))
        assert mapper.map("1.5E-3") == Decimal("0.0015")

    def test_number_column_maps_negative_value_with_exponent(self):
        mapper = RowMapperFactory()._create_value_mapper(_column("number"))
        assert mapper.map("-2.5E-2") == Decimal("-0.025")

    def test_number_column_maps_none(self):
        mapper = RowMapperFactory()._create_value_mapper(_column("number"))
        assert mapper.map(None) is None

    def test_number_column_maps_positive_infinity(self):
        mapper = RowMapperFactory()._create_value_mapper(_column("number"))
        assert mapper.map("+Infinity") == Decimal("Infinity")

    def test_number_column_maps_negative_infinity(self):
        mapper = RowMapperFactory()._create_value_mapper(_column("number"))
        assert mapper.map("-Infinity") == Decimal("-Infinity")

    def test_number_column_maps_nan(self):
        mapper = RowMapperFactory()._create_value_mapper(_column("number"))
        assert mapper.map("NaN").is_nan()

    def test_row_mapper_maps_number_column(self):
        columns = [{"typeSignature": _column("number")}]
        row_mapper = RowMapperFactory().create(columns=columns, legacy_primitive_types=False)
        assert row_mapper.map([["10.3"], [None]]) == [[Decimal("10.3")], [None]]

    def test_legacy_primitive_types_skips_mapping(self):
        columns = [{"typeSignature": _column("number")}]
        row_mapper = RowMapperFactory().create(columns=columns, legacy_primitive_types=True)
        assert row_mapper.map([["10.3"]]) == [["10.3"]]


class TestNumberCornerCases:
    """Corner cases specific to the Trino NUMBER type, which (unlike DECIMAL)
    is arbitrary-precision and carries IEEE-style special values."""

    @pytest.fixture
    def mapper(self):
        return RowMapperFactory()._create_value_mapper(_column("number"))

    def test_zero(self, mapper):
        assert mapper.map("0") == Decimal("0")

    def test_negative_zero_preserves_sign(self, mapper):
        result = mapper.map("-0")
        assert result == Decimal("0")
        assert result.is_signed()

    def test_nan_is_not_equal_to_itself(self, mapper):
        # NaN never compares equal, so callers must use is_nan()
        result = mapper.map("NaN")
        assert result.is_nan()
        assert result != result

    def test_arbitrary_precision_beyond_decimal38(self, mapper):
        # NUMBER is not bounded to DECIMAL's 38 significant digits
        digits = "1" * 60
        result = mapper.map(digits)
        assert result == Decimal(digits)
        assert len(result.as_tuple().digits) == 60

    def test_very_large_magnitude_exponent(self, mapper):
        value = "1.23456789012345678901234567890123456789012345678901E+16434"
        assert mapper.map(value) == Decimal(value)

    def test_very_small_magnitude_exponent(self, mapper):
        value = "-1.23456789012345678901234567890123456789012345678901E-16333"
        assert mapper.map(value) == Decimal(value)


def test_parametric_interval_advertised_in_capabilities():
    capabilities = constants.CLIENT_CAPABILITIES.split(",")
    assert constants.CLIENT_CAPABILITY_PARAMETRIC_INTERVAL in capabilities


@pytest.mark.parametrize("value, expected", [
    pytest.param(None, None, id="null"),
    # legacy format: always three fractional digits, holding milliseconds
    pytest.param("1 11:11:11.117", timedelta(days=1, seconds=40271, microseconds=117000), id="legacy"),
    pytest.param("-5 23:59:57.000", timedelta(days=-6, seconds=3), id="legacy-negative"),
    pytest.param("0 00:00:00.005", timedelta(milliseconds=5), id="legacy-zero-padded"),
    # PARAMETRIC_INTERVAL format: as many fractional digits as the type's precision
    pytest.param("2 00:00:00", timedelta(days=2), id="precision-0"),
    pytest.param("-0 00:00:02", timedelta(seconds=-2), id="precision-0-negative-zero-days"),
    pytest.param("0 00:00:01.5", timedelta(seconds=1, microseconds=500000), id="precision-1"),
    pytest.param("0 00:00:32.1239", timedelta(seconds=32, microseconds=123900), id="precision-4"),
    pytest.param("1 11:11:11.116555", timedelta(days=1, seconds=40271, microseconds=116555), id="precision-6"),
    pytest.param("-1 11:11:11.116555", -timedelta(days=1, seconds=40271, microseconds=116555),
                 id="precision-6-negative"),
    # digits beyond microseconds are rounded half to even, symmetrically for negative values
    pytest.param("1 00:00:00.123456789012", timedelta(days=1, microseconds=123457), id="precision-12"),
    pytest.param("0 00:00:00.0000005", timedelta(0), id="tie-rounds-to-even-down"),
    pytest.param("0 00:00:00.0000015", timedelta(microseconds=2), id="tie-rounds-to-even-up"),
    pytest.param("-0 00:00:00.0000015", timedelta(microseconds=-2), id="tie-negative"),
    pytest.param("0 00:00:00.9999995", timedelta(seconds=1), id="rounding-carries-into-seconds"),
    # range limits of servers storing microseconds in a long
    pytest.param("106751991 04:00:54.775807",
                 timedelta(days=106751991, hours=4, seconds=54, microseconds=775807), id="server-max"),
    pytest.param("-106751991 04:00:54.775808",
                 -timedelta(days=106751991, hours=4, seconds=54, microseconds=775808), id="server-min"),
    # range limits of timedelta, reachable on servers storing milliseconds in a long
    pytest.param("999999999 23:59:59.999",
                 timedelta(days=999999999, hours=23, minutes=59, seconds=59, milliseconds=999), id="python-max"),
    pytest.param("-999999999 00:00:00.000", timedelta(days=-999999999), id="python-min"),
])
def test_interval_day_to_second_mapper(value, expected):
    assert IntervalDayToSecondMapper().map(value) == expected


@pytest.mark.parametrize("value", [
    "1000000000 00:00:00",
    "-999999999 00:00:00.001",
])
def test_interval_day_to_second_mapper_out_of_python_range(value):
    with pytest.raises(TrinoDataError, match="exceeds the maximum or minimum limit"):
        IntervalDayToSecondMapper().map(value)


def _parametric_interval_day_to_second(start, end, leading_precision, fractional_precision):
    """typeSignature of a day-time interval as sent to clients supporting PARAMETRIC_INTERVAL."""
    arguments = [start, end, leading_precision, fractional_precision]
    return _column("interval day to second", [{"kind": "LONG", "value": a} for a in arguments])


def test_parametric_interval_day_to_second_nested_in_structural_types():
    day = _parametric_interval_day_to_second(2, 2, 1, 0)  # INTERVAL DAY(1)
    second = _parametric_interval_day_to_second(5, 5, 1, 1)  # INTERVAL SECOND(1, 1)
    microsecond = _parametric_interval_day_to_second(2, 5, 2, 6)  # INTERVAL DAY(2) TO SECOND(6)
    signature = _column("row", [
        {"kind": "NAMED_TYPE", "value": {"fieldName": {"name": "a"}, "typeSignature": _column(
            "array", [{"kind": "TYPE", "value": second}])}},
        {"kind": "NAMED_TYPE", "value": {"fieldName": {"name": "m"}, "typeSignature": _column(
            "map", [{"kind": "TYPE", "value": day}, {"kind": "TYPE", "value": microsecond}])}},
    ])
    mapper = RowMapperFactory()._create_value_mapper(signature)

    result = mapper.map([["0 00:00:01.5"], {"1 00:00:00": "0 00:00:01.250000"}])

    assert result.a == [timedelta(seconds=1, microseconds=500000)]
    assert result.m == {timedelta(days=1): timedelta(seconds=1, microseconds=250000)}
