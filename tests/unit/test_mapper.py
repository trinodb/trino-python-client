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
from decimal import Decimal

import pytest

from trino import constants
from trino.mapper import DecimalValueMapper
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
