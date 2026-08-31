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
"""Asynchronous Trino client; see :mod:`trino.aio.dbapi` for usage."""
from trino.aio.client import AsyncSegmentIterator
from trino.aio.client import AsyncSpooledSegment
from trino.aio.client import AsyncTrinoQuery
from trino.aio.client import AsyncTrinoRequest
from trino.aio.client import AsyncTrinoResult
from trino.aio.dbapi import connect
from trino.aio.dbapi import Connection
from trino.aio.dbapi import Cursor
from trino.aio.dbapi import SegmentCursor

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "SegmentCursor",
    "AsyncTrinoQuery",
    "AsyncTrinoRequest",
    "AsyncTrinoResult",
    "AsyncSegmentIterator",
    "AsyncSpooledSegment",
]
