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
"""Render Python temporal values as Trino SQL literals."""
import datetime
from zoneinfo import ZoneInfo


def format_temporal_literal(param: datetime.date) -> str:
    if isinstance(param, datetime.datetime) and param.tzinfo is None:
        datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
        return "TIMESTAMP '%s'" % datetime_str

    if isinstance(param, datetime.datetime) and param.tzinfo is not None:
        datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
        # named timezones
        if isinstance(param.tzinfo, ZoneInfo):
            return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.key)
        # offset-based timezones
        return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.tzname(param))

    # We can't calculate the offset for a time without a point in time
    if isinstance(param, datetime.time) and param.tzinfo is None:
        time_str = param.strftime("%H:%M:%S.%f")
        return "TIME '%s'" % time_str

    if isinstance(param, datetime.time) and param.tzinfo is not None:
        time_str = param.strftime("%H:%M:%S.%f")
        # named timezones
        if isinstance(param.tzinfo, ZoneInfo):
            utc_offset = datetime.datetime.now(tz=param.tzinfo).strftime('%z')
            return "TIME '%s %s:%s'" % (time_str, utc_offset[:3], utc_offset[3:])
        # offset-based timezones
        return "TIME '%s %s'" % (time_str, param.strftime('%Z')[3:])

    date_str = param.strftime("%Y-%m-%d")
    return "DATE '%s'" % date_str
