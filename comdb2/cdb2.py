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

"""This module provides a thin, pythonic wrapper over cdb2api.

Overview
========

Basic Usage
-----------

The main class used for interacting with a Comdb2 is `Handle`.  A basic usage
example looks like this::

    from comdb2 import cdb2
    hndl = cdb2.Handle('mattdb')
    for row in hndl.execute("select 1, 'a' union all select 2, 'b'"):
        print(row)

Which would result in the following output::

    [1, 'a']
    [2, 'b']

Graceful Teardown and Error Handling
------------------------------------

Non-trivial applications should guarantee that the handle is closed when it is
no longer needed, preferably by using `contextlib.closing`.  They should also
be prepared to handle any errors returned by the database.  Failures that are
encountered when connecting to or querying the database are raised as instances
of the `Error` class.  So, a more thorough version of the example above would
be::

    from comdb2 import cdb2
    import contextlib
    try:
        with contextlib.closing(cdb2.Handle('mattdb')) as hndl:
            for row in hndl.execute("select 1, 'a' union all select 2, 'b'"):
                print(row)
    except cdb2.Error as exc:
        print("Comdb2 exception encountered: %s" % exc)

In this example, `contextlib.closing` is used to guarantee that `Handle.close`
is called at the end of the ``with`` block, and an exception
handler been added for exceptions of type `Error`.

Controlling the Type Used For Result Rows
-----------------------------------------

As you can see, rows are returned as a `list` of column values in positional
order.  If you'd prefer to get the columns back as some other type, you can set
`Handle.row_factory` to one of the factories provided by
`comdb2.factories` - for example::

    from comdb2 import cdb2
    from comdb2 import factories
    hndl = cdb2.Handle('mattdb')
    hndl.row_factory = factories.dict_row_factory
    for row in hndl.execute("select 1 as 'x', 2 as 'y' union all select 3, 4"):
        print(row)

This program will return each row as a `dict` rather than a `list`::

    {'y': 2, 'x': 1}
    {'y': 4, 'x': 3}

Parameter Binding
-----------------

In real applications you'll often need to pass parameters into a SQL query.
This is done using parameter binding - in the query, placeholders are specified
using ``@name``, and a mapping of names to values is passed to `Handle.execute`
along with the query.  For example:

    >>> query = "select 25 between @a and @b"
    >>> print(list(hndl.execute(query, {'a': 20, 'b': 42})))
    [[1]]
    >>> params = {'a': 20, 'b': 23}
    >>> print(list(hndl.execute(query, params)))
    [[0]]

In this example, we run the query with two different sets of
parameters, producing different results.  First, we execute
the query with ``@a`` bound to ``20`` and ``@b`` bound to ``42``.  In this
case, because ``20 <= 25 <= 42``, the expression evaluates to true, and a ``1``
is returned.

When we run the same query with ``@b`` bound to ``23``, a ``0``
is returned instead, because ``20 <= 25 <= 23`` is false.  In both of these
examples we make use of the `list` constructor to turn the iterable returned
by `Handle.execute` into a list of result rows.

Types
-----

For all Comdb2 types, the same Python type is used for binding a parameter
value as is returned for a SQL query result column of that type.  In brief, SQL
types are mapped to Python types according to the following table:

============   ================================================================
SQL type       Python type
============   ================================================================
NULL           ``None``
integer        `int`
real           `float`
blob           `bytes`
text           `str`
datetime       `datetime.datetime`
datetimeus     `DatetimeUs`
============   ================================================================

See :ref:`Comdb2 to Python Type Mappings` for a thorough explanation of these
type mappings and their implications.

Note:
    This module uses byte strings to represent BLOB columns, and Unicode
    strings to represent TEXT columns.  This is a very common source of
    problems for new users.
    Make sure to carefully read :ref:`String and Blob Types` on that page.
"""

from __future__ import annotations

import array
import datetime
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, List, Tuple, Union
from ._cdb2_types import Error, Effects, DatetimeUs, ColumnType, ConnectionFlags
from ._ccdb2 import Handle as CHandle

__all__ = [
    "Error",
    "Handle",
    "Effects",
    "DatetimeUs",
    "ERROR_CODE",
    "TYPE",
    "HANDLE_FLAGS",
    "ColumnType",
    "ConnectionFlags",
    "Value",
    "ParameterValue",
]

Value = Union[
    None,
    int,
    float,
    bytes,
    str,
    datetime.datetime,
    DatetimeUs,
]
ParameterValue = Union[
    None,
    int,
    float,
    bytes,
    str,
    datetime.datetime,
    DatetimeUs,
    List[int],
    List[float],
    List[bytes],
    List[str],
    Tuple[int, ...],
    Tuple[float, ...],
    Tuple[bytes, ...],
    Tuple[str, ...],
]
Row = Any

# Pull all comdb2 error codes from cdb2api.h into our namespace
ERROR_CODE = {
    "CONNECT_ERROR": -1,
    "NOTCONNECTED": -2,
    "PREPARE_ERROR": -3,
    "IO_ERROR": -4,
    "INTERNAL": -5,
    "NOSTATEMENT": -6,
    "BADCOLUMN": -7,
    "BADSTATE": -8,
    "ASYNCERR": -9,
    "INVALID_ID": -12,
    "RECORD_OUT_OF_RANGE": -13,
    "REJECTED": -15,
    "STOPPED": -16,
    "BADREQ": -17,
    "DBCREATE_FAILED": -18,
    "THREADPOOL_INTERNAL": -20,
    "READONLY": -21,
    "NOMASTER": -101,
    "UNTAGGED_DATABASE": -102,
    "CONSTRAINTS": -103,
    "DEADLOCK": 203,
    "TRAN_IO_ERROR": -105,
    "ACCESS": -106,
    "TRAN_MODE_UNSUPPORTED": -107,
    "VERIFY_ERROR": 2,
    "FKEY_VIOLATION": 3,
    "NULL_CONSTRAINT": 4,
    "CONV_FAIL": 113,
    "NONKLESS": 114,
    "MALLOC": 115,
    "NOTSUPPORTED": 116,
    "DUPLICATE": 299,
    "TZNAME_FAIL": 401,
    "UNKNOWN": 300,
}
"""This dict maps all known Comdb2 error names to their respective values.

The value returned in `Error.error_code` will generally be the value
corresponding to one of the keys in this dict, though that's not always
guaranteed because new error codes can be added to the Comdb2 server at any
time.
"""

TYPE = {e.name: e.value for e in ColumnType}
"""This dict maps all known Comdb2 types to their enumeration value.

It predates the `ColumnType` enum and is retained only for backwards
compatibility. The `ColumnType` enum should be preferred for new usage.
"""

HANDLE_FLAGS = {e.name: e.value for e in ConnectionFlags}
"""This dict maps connection flags to their enumeration value.

It predates the `ConnectionFlags` enum and is retained only for backwards
compatibility. The `ConnectionFlags` enum should be preferred for new usage.
"""


class Handle:
    """Represents a connection to a database.

    By default, the connection will be made to the cluster configured as the
    machine-wide default for the given database.  This is almost always what
    you want.  If you need to connect to a database that's running on your
    local machine rather than a cluster, you can pass "local" as the ``tier``.
    It's also permitted to specify "dev", "alpha", "beta", or "prod" as the
    ``tier``, which will connect you directly to the tier you specify (firewall
    permitting).

    Alternately, you can pass a machine name as the ``host`` argument, to
    connect directly to an instance of the given database on that host, rather
    than on a cluster or the local machine.

    By default, the connection will use UTC as its timezone.  This differs from
    cdb2api's default behavior, where the timezone used by the query differs
    depending on the machine that it is run from.  If for some reason you need
    to have that machine-specific default timezone instead, you can pass
    ``None`` for the ``tz`` argument.  Any other valid timezone name may also
    be used instead of 'UTC'.

    Note that Python does not guarantee that object finalizers will be called
    when the interpreter exits, so to ensure that the handle is cleanly
    released you should call the `close` method when you're done with it.  You
    can use `contextlib.closing` to guarantee the handle is released when
    a block completes.

    Args:
        database_name (str): The name of the database to connect to.
        tier (str): The cluster to connect to.
        host (str): Alternately, a single remote host to connect to.
        flags (int): A flags value passed directly through to cdb2_open.
        tz (str): The timezone to be used by the new connection, or ``None`` to
            use a machine-specific default.
    """

    def __init__(
        self,
        database_name: str | bytes,
        tier: str | bytes = "default",
        flags: int = 0,
        tz: str = "UTC",
        host: str | bytes | None = None,
    ) -> None:
        if host is not None:
            if tier != "default":
                raise Error(
                    ERROR_CODE["NOTSUPPORTED"],
                    "Connecting to a host by name and to a "
                    "cluster by tier are mutually exclusive",
                )
            else:
                tier = host
                flags |= HANDLE_FLAGS["DIRECT_CPU"]

        self._hndl = CHandle(database_name, tier, flags)
        if tz is not None:
            self._hndl.execute("set timezone %s" % tz, {})
        self._cursor = iter([])

    def close(self, ack_current_event: bool = True) -> None:
        """Gracefully close the Comdb2 connection.

        Once a `Handle` has been closed, no further operations may be performed
        on it.

        If the handle was used to consume events from a `Lua consumer`__, then
        *ack_current_event* tells the database what to do with the last
        event that was delivered. By default it will be marked as consumed and
        won't be redelivered, but if ``ack_current_event=False`` then the
        event will be redelivered to another consumer for processing.

        __ https://bloomberg.github.io/comdb2/triggers.html#lua-consumers

        If a socket pool is running on the machine and the connection was in
        a clean state, this will turn over the connection to the socket pool.
        This cannot be done if the connection is in a transaction, or
        in the middle of retrieving a result set.  Other restrictions may apply
        as well.

        You can ensure that this gets called at the end of a block using
        something like:

            >>> with contextlib.closing(Handle('mattdb')) as hndl:
            >>>     for row in hndl.execute("select 1"):
            >>>         print(row)
            [1]
        """
        self._hndl.close(ack_current_event=ack_current_event)

    @property
    def row_factory(self) -> Callable[[list[str]], Callable[[list[Value]], Row]]:
        """Factory used when constructing result rows.

        By default, or when set to ``None``, each row is returned as a `list`
        of column values.  If you'd prefer to receive rows as a `dict` or as
        a `collections.namedtuple`, you can set this property to one of the
        factories provided by the `comdb2.factories` module.

        Example:
            >>> from comdb2.factories import dict_row_factory
            >>> hndl.row_factory = dict_row_factory
            >>> for row in hndl.execute("SELECT 1 as 'foo', 2 as 'bar'"):
            ...     print(row)
            {'foo': 1, 'bar': 2}

        .. versionadded:: 0.9
        """
        return self._hndl.row_factory

    @row_factory.setter
    def row_factory(
        self, value: Callable[[list[str]], Callable[[list[Value]], Row]]
    ) -> None:
        self._hndl.row_factory = value

    def execute(
        self,
        sql: str | bytes,
        parameters: Mapping[str, ParameterValue] | None = None,
        *,
        column_types: Sequence[ColumnType] | None = None,
    ) -> Handle:
        """Execute a database operation (query or command).

        The ``sql`` string may have placeholders for parameters to be passed.
        This should always be the preferred method of parameterizing the SQL
        query, as it prevents SQL injection vulnerabilities and is faster.
        Placeholders for named parameters must be in Comdb2's native format,
        ``@param_name``.

        If ``column_types`` is provided and non-empty, it must be a sequence of
        members of the `ColumnType` enumeration. The database will coerce the
        data in the Nth column of the result set to the Nth given column type.
        An error will be raised if the number of elements in ``column_types``
        doesn't match the number of columns in the result set, or if one of the
        elements is not a supported column type, or if coercion fails. If
        ``column_types`` is empty or not provided, no coercion is performed.

        Args:
            sql (str): The SQL string to execute.
            parameters (Mapping[str, Any]): An optional mapping from parameter
                names to the values to be bound for them.
            column_types (Sequence[int]): An optional sequence of types (values
                of the `ColumnType` enumeration) which the columns of the
                result set will be coerced to.

        Returns:
            Handle: This method returns the `Handle` that it was called on,
            which can be used as an iterator over the result set returned by
            the query.  Iterating over it will yield one ``list`` per row in
            the result set, where the elements in the list correspond to the
            result columns within the row, in positional order.

            The `row_factory` property can be used to return rows as
            a different type, instead.

        Example:
            >>> for row in hndl.execute("select 1, 2 UNION ALL select @x, @y",
            ...                         {'x': 2, 'y': 4}):
            ...     print(row)
            [1, 2]
            [2, 4]
        """
        if parameters is None:
            parameters = {}

        column_types_array: Sequence[int] | None = None
        if column_types is not None:
            column_types_array = array.array("i", column_types)
        if not column_types_array:
            column_types_array = None

        self._cursor = iter(self._hndl.execute(sql, parameters, column_types_array))
        return self

    def __iter__(self) -> Iterator[Row]:
        """Iterate over all remaining rows in the current result set.

        By default each row is returned as a `list`, where the elements in the
        list correspond to the result row's columns in positional order, but
        this can be changed with the `row_factory` property.

        Example:
            >>> hndl.execute("select 1, 2 UNION ALL select 3, 4")
            >>> for row in hndl:
            ...     print(row)
            [1, 2]
            [3, 4]
        """
        return self._cursor

    def __next__(self):
        # Allow using `Handle` as an iterator, to avoid breaking backwards
        # compatibility for a user who did something like:
        #   next(handle.execute("select 1"))
        return next(self._cursor)

    next = __next__

    def get_effects(self) -> Effects:
        """Return counts of rows affected by executed queries.

        Within a transaction, these counts are a running total from the start
        of the transaction up through the last executed SQL statement.  Outside
        of a transaction, these counts represent the rows affected by only the
        last executed SQL statement.

        Warning:
            Calling this method consumes all remaining rows in the current
            result set.

        Note:
            The results within a transaction are not necessarily reliable
            unless the ``VERIFYRETRY`` setting is turned off.  All of the
            caveats of the ``cdb2_get_effects`` call apply.

        Returns:
            Effects: An count of rows that have been affected, selected,
            updated, deleted, or inserted.
        """
        return self._hndl.get_effects()

    def column_names(self) -> list[str]:
        """Returns the names of the columns of the current result set.

        Returns:
            A list of unicode strings, one per column in the result set.
        """
        return self._hndl.column_names()

    def column_types(self) -> list[int]:
        """Returns the type codes of the columns of the current result set.

        Returns:
            List[int]: A list of integers, one per column in the result set.
            Each generally corresponds to one of the types in the `TYPE` global
            object exposed by this module.
        """
        return self._hndl.column_types()
