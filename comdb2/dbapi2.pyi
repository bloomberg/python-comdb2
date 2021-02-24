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

import datetime

from typing import Any
from typing import Callable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Text
from typing import Tuple
from typing import Union

from . import cdb2

apilevel: str
threadsafety: int
paramstyle: str

STRING: object
BINARY: object
NUMBER: object
DATETIME: object
ROWID: object


def Binary(string: Union[Text, bytes]) -> bytes: ...


Datetime = datetime.datetime
DatetimeUs = cdb2.DatetimeUs
Timestamp = Datetime
TimestampUs = DatetimeUs

DatetimeFromTicks = Datetime.fromtimestamp
DatetimeUsFromTicks = DatetimeUs.fromtimestamp
TimestampFromTicks = Timestamp.fromtimestamp
TimestampUsFromTicks = TimestampUs.fromtimestamp

Value = cdb2.Value
Row = cdb2.Row


class Error(Exception):
    ...


class Warning(Exception):
    ...


class InterfaceError(Error):
    ...


class DatabaseError(Error):
    ...


class InternalError(DatabaseError):
    ...


class OperationalError(DatabaseError):
    ...


class ProgrammingError(DatabaseError):
    ...


class IntegrityError(DatabaseError):
    ...


class UniqueKeyConstraintError(IntegrityError):
    ...


class ForeignKeyConstraintError(IntegrityError):
    ...


class NonNullConstraintError(IntegrityError):
    ...


class DataError(DatabaseError):
    ...


class NotSupportedError(DatabaseError):
    ...


def connect(
    database_name: Union[Text, bytes],
    tier: Union[Text, bytes] = ...,
    autocommit: bool = ...,
    host: Optional[Union[Text, bytes]] = ...,
) -> Connection: ...


class Connection:
    def __init__(
        self,
        database_name: Union[Text, bytes],
        tier: Union[Text, bytes] = ...,
        autocommit: bool = ...,
        host: Optional[Union[Text, bytes]] = ...,
    ) -> None: ...

    @property
    def row_factory(self) -> Callable[[List[Text]], Callable[[List[Value]], Row]]: ...

    @row_factory.setter
    def row_factory(
        self, value: Callable[[List[Text]], Callable[[List[Value]], Row]]
    ) -> None: ...

    def close(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def cursor(self) -> Cursor: ...

    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    InternalError = InternalError
    OperationalError = OperationalError
    ProgrammingError = ProgrammingError
    IntegrityError = IntegrityError
    DataError = DataError
    NotSupportedError = NotSupportedError


class Cursor:
    @property
    def arraysize(self) -> int: ...

    @arraysize.setter
    def arraysize(self, value: int) -> None: ...

    @property
    def description(self) -> Tuple[Text, object, None, None, None, None, None]: ...

    @property
    def rowcount(self) -> int: ...

    @property
    def connection(self) -> Connection: ...

    def close(self) -> None: ...

    def callproc(
        self,
        procname: Union[str, Text],
        parameters: Sequence[Value],
    ) -> Sequence[Value]: ...

    def execute(
        self,
        sql: Union[str, Text],
        parameters: Optional[Mapping[Union[str, Text], Value]] = None,
    ) -> Cursor: ...

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Sequence[Mapping[Union[str, Text], Value]],
    ) -> None: ...

    def setinputsizes(self, sizes: Sequence[Any]) -> None: ...

    def setoutputsize(self, size: Any, column: int = ...) -> None: ...

    def fetchone(self) -> Optional[Row]: ...

    def fetchmany(self, n: Optional[int] = ...) -> Sequence[Row]: ...

    def fetchall(self) -> Sequence[Row]: ...

    def __iter__(self) -> Iterator[Row]: ...
