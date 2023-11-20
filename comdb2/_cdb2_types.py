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

from collections import namedtuple
from datetime import datetime
import six

__name__ = 'comdb2.cdb2'


def _errstr(msg):
    try:
        return msg.decode('utf-8')
    except UnicodeDecodeError:
        # The DB's error strings aren't necessarily UTF-8.
        # If one isn't, it's preferable to mangle the error string than to
        # raise a UnicodeDecodeError (which would obscure the root cause).
        # Return a unicode string with \x escapes in place of non-ascii bytes.
        return msg.decode('latin1').encode('unicode_escape').decode('ascii')


class Error(RuntimeError):
    """Exception type raised for all failed operations.

    Attributes:
        error_code (int): The error code from the failed cdb2api call.
        error_message (str): The string returned by cdb2api's ``cdb2_errstr``
            after the failed call.
    """
    def __init__(self, error_code, error_message):
        if not(isinstance(error_message, str)):
            error_message = _errstr(error_message)
        self.error_code = error_code
        self.error_message = error_message
        super().__init__(error_code, error_message)


class Effects(namedtuple('Effects',
    "num_affected num_selected num_updated num_deleted num_inserted")):
    """Type used to represent the count of rows affected by a SQL query.

    An object of this type is returned by `Handle.get_effects`.

    Attributes:
        num_affected (int): The total number of rows that were affected.
        num_selected (int): The number of rows that were selected.
        num_updated (int): The number of rows that were updated.
        num_deleted (int): The number of rows that were deleted.
        num_inserted (int): The number of rows that were inserted.
    """
    __slots__ = ()


class DatetimeUs(datetime):
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
    def fromdatetime(cls, dt):
        """Return a `DatetimeUs` copied from a given `datetime.datetime`"""
        fold = getattr(dt, 'fold', None)
        kwargs = {}
        if fold is not None:
            kwargs['fold'] = fold

        return DatetimeUs(dt.year, dt.month, dt.day,
                          dt.hour, dt.minute, dt.second, dt.microsecond,
                          dt.tzinfo, **kwargs)

    def __add__(self, other):
        ret = super().__add__(other)
        if isinstance(ret, datetime):
            return DatetimeUs.fromdatetime(ret)
        return ret  # must be a timedelta

    def __sub__(self, other):
        ret = super().__sub__(other)
        if isinstance(ret, datetime):
            return DatetimeUs.fromdatetime(ret)
        return ret  # must be a timedelta

    def __radd__(self, other):
        return self + other

    @classmethod
    def now(cls, tz=None):
        ret = super().now(tz)
        return DatetimeUs.fromdatetime(ret)

    @classmethod
    def fromtimestamp(cls, timestamp, tz=None):
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
