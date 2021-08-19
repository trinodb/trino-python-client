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

from trino.transaction import IsolationLevel
import pytest


def test_isolation_level_levels() -> None:
    levels = {
        "AUTOCOMMIT",
        "READ_UNCOMMITTED",
        "READ_COMMITTED",
        "REPEATABLE_READ",
        "SERIALIZABLE",
    }

    assert IsolationLevel.levels() == levels


def test_isolation_level_values() -> None:
    values = {
        0, 1, 2, 3, 4
    }

    assert IsolationLevel.values() == values


def test_isolation_level_check_match() -> None:
    assert IsolationLevel.check(3) == 3


def test_isolation_level_check_mismatch() -> None:
    with pytest.raises(ValueError):
        IsolationLevel.check(-1)
