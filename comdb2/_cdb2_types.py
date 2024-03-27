# Copyright 2017 Bloomberg Finance L.P.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import datetime
import enum
from typing import NamedTuple

__name__ = "comdb2.cdb2"


def _errstr(msg):
    try:
        return msg.decode("utf-8")
    except UnicodeDecodeError:
        # The DB's error strings aren't necessarily UTF-8.
        # If one isn't, it's preferable to mangle the error string than to
        # raise a UnicodeDecodeError (which would obscure the root cause).
        # Return a unicode string with \x escapes in place of non-ascii bytes.
        return msg.decode("latin1").encode("unicode_escape").decode("ascii")


class Error(RuntimeError):
    """Exception type raised for all failed operations.

    Attributes:
        error_code (int): The error code from the failed cdb2api call.
        error_message (str): The string returned by cdb2api's ``cdb2_errstr``
            after the failed call.
    """

    def __init__(self, error_code: int, error_message: str) -> None:
        if not (isinstance(error_message, str)):
            error_message = _errstr(error_message)
        self.error_code = error_code
        self.error_message = error_message
        super().__init__(error_code, error_message)


class Effects(NamedTuple):
    """Type used to represent the count of rows affected by a SQL query.

    An object of this type is returned by `Handle.get_effects`.
    """

    num_affected: int
    num_selected: int
    num_updated: int
    num_deleted: int
    num_inserted: int


# Override the auto-generated docstrings with more informative ones.
Effects.num_affected.__doc__ = "The total number of rows that were affected."
Effects.num_selected.__doc__ = "The number of rows that were selected."
Effects.num_updated.__doc__ = "The number of rows that were updated."
Effects.num_deleted.__doc__ = "The number of rows that were deleted."
Effects.num_inserted.__doc__ = "The number of rows that were inserted."


class DatetimeUs(datetime.datetime):
    """Provides a distinct representation for Comdb2's DATETIMEUS type.

    Historically, Comdb2 provided a DATETIME type with millisecond precision.
    Comdb2 R6 added a DATETIMEUS type, which instead has microsecond precision.

    This module represents each Comdb2 type with a distinct Python type.  For
    backwards compatibility with older Comdb2 databases, `datetime.datetime` is
    mapped to the DATETIME type, and this class to the DATETIMEUS type.
    Because this is a subclass of `datetime.datetime`, you don't need to do
    anything special when reading a DATETIMEUS type out of the database.  You
    can use `isinstance` if you need to check whether you've been given
    a `datetime.datetime` (meaning the column was of the DATETIME type) or
    a `DatetimeUs` (meaning the column was of the DATETIMEUS type), but all the
    same methods will work on either.

    When binding a parameter of type DATETIMEUS, you must pass an instance of
    this class, as a `datetime.datetime` would instead be bound as a DATETIME.
    Instances of this class can be created using this constructor, or the
    `.fromdatetime` alternate constructor, or any of the other alternate
    constructors inherited from `datetime.datetime`.
    """

    @classmethod
    def fromdatetime(cls, dt: datetime.datetime) -> DatetimeUs:
        """Return a `DatetimeUs` copied from a given `datetime.datetime`"""
        fold = getattr(dt, "fold", None)
        kwargs = {}
        if fold is not None:
            kwargs["fold"] = fold

        return DatetimeUs(
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            dt.microsecond,
            dt.tzinfo,
            **kwargs,
        )

    def __add__(self, other: datetime.timedelta) -> DatetimeUs:
        ret = super().__add__(other)
        if isinstance(ret, datetime.datetime):
            return DatetimeUs.fromdatetime(ret)
        return ret  # must be a timedelta

    def __sub__(self, other: datetime.timedelta) -> DatetimeUs:
        ret = super().__sub__(other)
        if isinstance(ret, datetime.datetime):
            return DatetimeUs.fromdatetime(ret)
        return ret  # must be a timedelta

    def __radd__(self, other: datetime.timedelta) -> DatetimeUs:
        return self + other

    @classmethod
    def now(cls, tz: datetime.tzinfo | None = None) -> DatetimeUs:
        ret = super().now(tz)
        return DatetimeUs.fromdatetime(ret)

    @classmethod
    def fromtimestamp(
        cls, timestamp: float, tz: datetime.tzinfo | None = None
    ) -> DatetimeUs:
        ret = super().fromtimestamp(timestamp, tz)
        return DatetimeUs.fromdatetime(ret)

    def astimezone(self, *args, **kwargs):
        ret = super().astimezone(*args, **kwargs)
        return DatetimeUs.fromdatetime(ret)

    def replace(self, *args, **kwargs):
        # Before Python 3.7, it is effectively implementation dependent whether
        # this returns a DatetimeUs or a datetime.
        dt = super().replace(*args, **kwargs)
        return self.fromdatetime(dt)


class ColumnType(enum.IntEnum):
    """This enum represents all known Comdb2 column types.

    Each value in the list returned by `Handle.column_types` will generally be
    the value corresponding to one of the members of this enumeration, though
    that's not always guaranteed because new types can be added to the Comdb2
    server at any time.

    A sequence of consisting of members of this enum can be passed as the
    *column_types* parameter of `Handle.execute`. The database will coerce the
    returned data to the given column types, or return an error if it cannot.
    """

    # The column types from cdb2_coltype in cdb2api.h, plus a few aliases.
    INTEGER = 1
    REAL = 2
    CSTRING = 3
    BLOB = 4
    DATETIME = 6
    INTERVALYM = 7
    INTERVALDS = 8
    DATETIMEUS = 9
    INTERVALDSUS = 10
    INT = INTEGER
    FLOAT = REAL
    STRING = CSTRING
    TEXT = CSTRING
    BYTES = BLOB


class ConnectionFlags(enum.IntEnum):
    """This enum represents connection flags.

    These values can be passed to the `Handle` constructor, either individually
    or as a bitwise OR of multiple flags.
    """

    # The flags from cdb2_hndl_alloc_flags in cdb2api.h
    READ_INTRANS_RESULTS = 2
    DIRECT_CPU = 4
    RANDOM = 8
    RANDOMROOM = 16
    ROOM = 32
