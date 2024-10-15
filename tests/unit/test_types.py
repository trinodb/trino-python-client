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

import pickle
from datetime import datetime, time
from decimal import Decimal

import pytest

from trino import types


def identity(x):
    return x


type_instances = [
    (types.Time(time(11, 47, 23), Decimal(0.314)), lambda v: v.to_python_type()),
    (types.TimeWithTimeZone(time(11, 47, 23), Decimal(0.314)), lambda v: v.to_python_type()),
    (types.Timestamp(datetime(2024, 10, 15, 11, 47, 23), Decimal(0.314)), lambda v: v.to_python_type()),
    (types.TimestampWithTimeZone(datetime(2024, 10, 15, 11, 47, 23), Decimal(0.314)), lambda v: v.to_python_type()),
    (types.NamedRowTuple(["Alice", 38], ["name", "age"], ["varchar", "integer"]), identity),
]


@pytest.mark.parametrize("value,fn", type_instances)
def test_pickle_roundtripping(value, fn):
    bytes = pickle.dumps(value)
    unpickled_value = pickle.loads(bytes)
    assert fn(value) == fn(unpickled_value)
