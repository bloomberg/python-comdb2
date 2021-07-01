# Copyright 2021 Bloomberg Finance L.P.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Text
from typing import Union
from typing import overload


class Error(RuntimeError):
    error_code: int
    error_message: str

    def __init__(self, error_code: int, error_message: str) -> None: ...


class Effects(NamedTuple):
    num_affected: int
    num_selected: int
    num_updated: int
    num_deleted: int
    num_inserted: int


class DatetimeUs(datetime):
    @classmethod
    def fromdatetime(cls, dt: datetime) -> DatetimeUs: ...

    def __add__(self, other: timedelta) -> DatetimeUs: ...

    # XXX: datetime derives from date, which provides a __sub__ for a date rhs.
    #      mypy doesn't like that datetime and its subclasses don't provide it.
    @overload  # type: ignore[override]
    def __sub__(
        self,
        other: datetime,
    ) -> timedelta: ...

    @overload
    def __sub__(self, other: timedelta) -> DatetimeUs: ...

    def __radd__(self, other: timedelta) -> DatetimeUs: ...

    @classmethod
    def now(cls, tz: Optional[tzinfo] = ...) -> DatetimeUs: ...

    @classmethod
    def fromtimestamp(
        cls, timestamp: float, tz: Optional[tzinfo] = ...
    ) -> DatetimeUs: ...

    if sys.version_info >= (3, 3):
        def astimezone(self, tz: Optional[tzinfo]=...) -> DatetimeUs: ...
    else:
        def astimezone(self, tz: tzinfo) -> DatetimeUs: ...

    if sys.version_info >= (3, 6):
        def replace(
            self,
            year: int = ...,
            month: int = ...,
            day: int = ...,
            hour: int = ...,
            minute: int = ...,
            second: int = ...,
            microsecond: int = ...,
            tzinfo: Optional[tzinfo] = ...,
            *,
            fold: int = ...,
        ) -> DatetimeUs: ...
    else:
        def replace(
            self,
            year: int = ...,
            month: int = ...,
            day: int = ...,
            hour: int = ...,
            minute: int = ...,
            second: int = ...,
            microsecond: int = ...,
            tzinfo: Optional[tzinfo] = ...,
        ) -> DatetimeUs: ...


ERROR_CODE: Dict[Text, int]
TYPE: Dict[Text, int]
HANDLE_FLAGS: Dict[Text, int]

Value = Union[None, int, float, bytes, Text, datetime, DatetimeUs]
Row = Any


class Handle:
    def __init__(
        self,
        database_name: Union[Text, bytes],
        tier: Union[Text, bytes] = ...,
        flags: int = ...,
        tz: str = ...,
        host: Optional[Union[Text, bytes]] = ...,
    ) -> None: ...

    def close(self) -> None: ...

    @property
    def row_factory(self) -> Callable[[List[Text]], Callable[[List[Value]], Row]]: ...

    @row_factory.setter
    def row_factory(
        self, value: Callable[[List[Text]], Callable[[List[Value]], Row]]
    ) -> None: ...

    def execute(
        self,
        sql: Union[Text, bytes],
        parameters: Optional[Mapping[Text, Value]] = ...,
    ) -> Handle: ...

    def __iter__(self) -> Iterator[Row]: ...

    def get_effects(self) -> Effects: ...

    def column_names(self) -> List[Text]: ...

    def column_types(self) -> List[int]: ...
