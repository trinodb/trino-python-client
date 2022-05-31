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
import pytest
from sqlalchemy.sql.sqltypes import ARRAY

from trino.sqlalchemy.datatype import MAP, ROW, SQLType, TIMESTAMP, TIME


@pytest.fixture(scope="session")
def assert_sqltype():
    def _assert_sqltype(this: SQLType, that: SQLType):
        if isinstance(this, type):
            this = this()
        if isinstance(that, type):
            that = that()

        assert type(this) == type(that)

        if isinstance(this, ARRAY):
            _assert_sqltype(this.item_type, that.item_type)
            if this.dimensions is None or this.dimensions == 1:
                # ARRAY(dimensions=None) == ARRAY(dimensions=1)
                assert that.dimensions is None or that.dimensions == 1
            else:
                assert that.dimensions == this.dimensions
        elif isinstance(this, MAP):
            _assert_sqltype(this.key_type, that.key_type)
            _assert_sqltype(this.value_type, that.value_type)
        elif isinstance(this, ROW):
            assert len(this.attr_types) == len(that.attr_types)
            for (this_attr, that_attr) in zip(this.attr_types, that.attr_types):
                assert this_attr[0] == that_attr[0]
                _assert_sqltype(this_attr[1], that_attr[1])

        elif isinstance(this, TIME):
            assert this.precision == that.precision
            assert this.timezone == that.timezone

        elif isinstance(this, TIMESTAMP):
            assert this.precision == that.precision
            assert this.timezone == that.timezone

        else:
            assert str(this) == str(that)

    return _assert_sqltype
