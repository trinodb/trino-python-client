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

from presto.client import get_header_values, get_session_property_values
from presto import constants


def test_get_header_values():
    headers = {constants.HEADER_SET_SESSION: "a, b"}
    values = get_header_values(headers, constants.HEADER_SET_SESSION)
    assert values == ["a", "b"]


def test_get_session_property_values():
    headers = {constants.HEADER_SET_SESSION: "a=1, b=2"}
    values = get_session_property_values(headers, constants.HEADER_SET_SESSION)
    assert values == [("a", "1"), ("b", "2")]
