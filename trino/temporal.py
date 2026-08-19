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
"""Render Python temporal values as Trino SQL literals.

The DBAPI parameter formatter and the SQLAlchemy literal processors share
these functions. A value therefore renders to the same SQL on both paths.
"""
import datetime
from typing import Optional


def format_temporal_literal(value: datetime.date) -> str:
    """Render a datetime, time, or date value as a Trino literal."""
    if isinstance(value, datetime.datetime):
        return format_timestamp_literal(value)
    if isinstance(value, datetime.time):
        return format_time_literal(value)
    return format_date_literal(value)


def format_timestamp_literal(value: datetime.datetime) -> str:
    body = value.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
    if value.tzinfo is None:
        return f"TIMESTAMP '{body}'"
    zone = _zone_id(value.tzinfo)
    if zone is None:
        # utcoffset() at this datetime preserves the instant. Only zone
        # identity is lost. No offset at all means the value is
        # naive - reject it rather than render a plain TIMESTAMP.
        offset = value.utcoffset()
        if offset is None:
            raise ValueError(f"tzinfo of {value!r} reports no UTC offset")
        zone = _format_offset(offset)
    return f"TIMESTAMP '{body} {zone}'"


def format_time_literal(value: datetime.time) -> str:
    """Render a time value as a Trino TIME literal.

    A Trino TIME WITH TIME ZONE literal accepts only a fixed offset. A time
    in a named zone resolves to an offset via the current date, so for DST
    zones the rendered offset varies with the day the query is built. Pass a
    fixed-offset tzinfo to render an exact literal.
    """
    # TODO: reject named-zone times instead of resolving via the current
    # date. That is a breaking change to the DBAPI parameter path.
    body = value.replace(tzinfo=None).isoformat(timespec="microseconds")
    if value.tzinfo is None:
        return f"TIME '{body}'"
    offset = value.utcoffset()
    if offset is None:
        # A named zone needs a date to resolve an offset. A time carries
        # no date so use today. For DST zones the result varies by date.
        offset = datetime.datetime.now(tz=value.tzinfo).utcoffset()
    if offset is None:
        raise ValueError(f"tzinfo of {value!r} reports no UTC offset")
    return f"TIME '{body} {_format_offset(offset)}'"


def format_date_literal(value: datetime.date) -> str:
    return f"DATE '{value.isoformat()}'"


def _zone_id(tzinfo: datetime.tzinfo) -> Optional[str]:
    # zoneinfo.ZoneInfo exposes the IANA zone name as .key, pytz zones as
    # .zone. Trino keeps the zone name in a TIMESTAMP WITH TIME ZONE value,
    # so prefer the name over a fixed offset.
    zone = getattr(tzinfo, "key", None) or getattr(tzinfo, "zone", None)
    if isinstance(zone, str) and zone:
        return zone
    return None


def _format_offset(offset: datetime.timedelta) -> str:
    seconds = round(offset.total_seconds())
    sign = "+" if seconds >= 0 else "-"
    minutes, remainder = divmod(abs(seconds), 60)
    if remainder:
        raise ValueError(f"a Trino literal cannot represent the sub-minute UTC offset {offset!r}")
    hours, minutes = divmod(minutes, 60)
    return f"{sign}{hours:02d}:{minutes:02d}"
