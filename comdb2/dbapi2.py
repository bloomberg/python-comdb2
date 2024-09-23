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

"""This module provides a DB-API 2.0 compatible Comdb2 API.

Overview
========

This module provides a Comdb2 interface that conforms to `the Python Database
API Specification v2.0 <https://www.python.org/dev/peps/pep-0249/>`_.

Basic Usage
-----------

The main class used for interacting with a Comdb2 is `Connection`, which you
create by calling the `connect` factory function.  Any errors that are
encountered when connecting to or querying the database are raised as instances
of the `Error` class.

A basic usage example looks like this::

    from comdb2 import dbapi2
    conn = dbapi2.connect('mattdb', autocommit=True)
    cursor = conn.cursor()
    cursor.execute("select 1, 'a' union all select 2, 'b'")
    for row in cursor.fetchall():
        print(row)

The above would result in the following output::

    [1, 'a']
    [2, 'b']

To reduce the amount of boilerplate required for fetching result sets, we
implement 2 extensions to the interface required by the Python DB-API: `Cursor`
objects are iterable, yielding one row of the result set per iteration, and
`Cursor.execute` returns the `Cursor` itself.  By utilizing these extensions,
the basic example can be shortened to::

    from comdb2 import dbapi2
    conn = dbapi2.connect('mattdb', autocommit=True)
    for row in conn.cursor().execute("select 1, 'a' union all select 2, 'b'"):
        print(row)

Graceful Teardown and Error Handling
------------------------------------

Non-trivial applications should guarantee that the `Connection` is closed when
it is no longer needed, preferably by using `contextlib.closing`.  They should
also be prepared to handle any errors returned by the database.  So, a more
thorough version of the example above would be::

    from comdb2 import dbapi2
    from contextlib import closing
    try:
        with closing(dbapi2.connect('mattdb', autocommit=True)) as conn:
            query = "select 1, 'a' union all select 2, 'b'"
            for row in conn.cursor().execute(query):
                print(row)
    except dbapi2.Error as exc:
        print("Comdb2 exception encountered: %s" % exc)

In this example, `contextlib.closing` is used to guarantee that
`Connection.close` is called at the end of the ``with`` block, and an exception
handler been added for exceptions of type `Error`.  All exceptions raised by
this module are subclasses of `Error`.  See :ref:`Exceptions` for details on
when each exception type is raised.

Controlling the Type Used For Result Rows
-----------------------------------------

As you can see, rows are returned as a `list` of column values in positional
order.  If you'd prefer to get the columns back as some other type, you can set
`Connection.row_factory` to one of the factories provided by
`comdb2.factories` - for example::

    from comdb2 import dbapi2
    from comdb2 import factories
    conn = dbapi2.connect('mattdb', autocommit=True)
    conn.row_factory = factories.dict_row_factory
    c = conn.cursor()
    for row in c.execute("select 1 as 'x', 2 as 'y' union all select 3, 4"):
        print(row)

This program will return each row as a `dict` rather than a `list`::

    {'y': 2, 'x': 1}
    {'y': 4, 'x': 3}

Parameter Binding
-----------------

In real applications you'll often need to pass parameters into a SQL query.
This is done using parameter binding - in the query, placeholders are specified
using ``%(name)s``, and a mapping of ``name`` to parameter value is passed to
`Cursor.execute` along with the query.  The ``%(`` and ``)s`` are fixed, and
the ``name`` between them varies for each parameter.  For example:

    >>> query = "select 25 between %(a)s and %(b)s"
    >>> print(conn.cursor().execute(query, {'a': 20, 'b': 42}).fetchall())
    [[1]]
    >>> params = {'a': 20, 'b': 23}
    >>> print(conn.cursor().execute(query, params).fetchall())
    [[0]]

In this example, we run the query with two different sets of
parameters, producing different results.  First, we execute the query with
parameter ``a`` bound to ``20`` and ``b`` bound to ``42``.  In this case,
because ``20 <= 25 <= 42``, the expression evaluates to true, and a ``1`` is
returned.

When we run the same query with parameter ``b`` bound to ``23``, a ``0`` is
returned instead, because ``20 <= 25 <= 23`` is false.

Note:
    Because parameters are bound using ``%(name)s``, other ``%`` signs in
    a query must be escaped.  For example, ``WHERE name like 'M%'`` becomes
    ``WHERE name LIKE 'M%%'``.

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

.. _Autocommit Mode:

Autocommit Mode
---------------

In all of the examples above, we gave the ``autocommit=True`` keyword argument
when calling `connect`.  This opts out of DB-API compliant transaction
handling, in order to use Comdb2's native transaction semantics.

By default, DB-API cursors are always in a transaction.  You can commit that
transaction using `Connection.commit`, or roll it back using
`Connection.rollback`.  For example::

    conn = dbapi2.connect('mattdb')
    cursor = conn.cursor()
    query = "insert into simple(key, val) values (%(key)s, %(val)s)"
    cursor.execute(query, {'key': 1, 'val': 2})
    cursor.execute(query, {'key': 3, 'val': 4})
    cursor.execute(query, {'key': 5, 'val': 6})
    conn.commit()

There are several things to note here.  The first is that the insert statements
that were sent to the database don't take effect immediately, because they are
implicitly part of a transaction that must be explicitly completed.  This is
different from other Comdb2 APIs, where you must execute a ``BEGIN`` statement
to start a transaction, and where statements otherwise take effect immediately.

The second thing to note is that there are certain error conditions where
a Comdb2 connection can automatically recover when outside of a transaction,
but not within a transaction.  In other words, using transactions when they
aren't needed can introduce new failure modes into your program.

In order to provide greater compatibility with other Comdb2 interfaces and
to eliminate the performance costs and extra error cases introduced by using
transactions unnecessarily, we allow you to pass the non-standard
``autocommit=True`` keyword argument when creating a new `Connection`. This
results in the implicit transaction not being created. You can still start
a transaction explicitly by passing a ``BEGIN`` statement to
`Cursor.execute`.  For example::

    conn = dbapi2.connect('mattdb', autocommit=True)
    cursor = conn.cursor()
    cursor.execute("delete from simple where 1=1")
    cursor.execute("begin")
    query = "insert into simple(key, val) values (%(key)s, %(val)s)"
    cursor.execute(query, {'key': 1, 'val': 2})
    cursor.execute(query, {'key': 3, 'val': 4})
    cursor.execute(query, {'key': 5, 'val': 6})
    cursor.execute("rollback")

In this example, because we've used ``autocommit=True`` the delete statement
takes effect immediately (that is, it is automatically committed).  We then
explicitly create a transaction, insert 3 rows, then decide not to commit
it, and instead explicitly roll back the transaction.

To summarize: you cannot use ``autocommit`` mode if you intend to pass the
`Connection` to a library that requires DB-API compliant connections.  You
should prefer ``autocommit`` mode when you don't want to use transactions (for
example, read-only queries where no particular consistency guarantees are
required across queries).  If you do intend to use transactions but won't pass
the `Connection` to a library that requires DB-API compliance, you can choose
either mode.  It may be easier to port existing code if you use ``autocommit``
mode, but avoiding ``autocommit`` mode may allow you to use 3rd party libraries
in the future that require DB-API compliant connections.

DB-API Compliance
-----------------

The interface this module provides fully conforms to `the Python Database API
Specification v2.0 <https://www.python.org/dev/peps/pep-0249/>`_ with a few
specific exceptions:

1. DB-API requires ``Date`` and ``DateFromTicks`` constructors, which we don't
   provide because Comdb2 has no type for representing a date without a time
   component.

2. DB-API requires ``Time`` and ``TimeFromTicks`` constructors, which we don't
   provide because Comdb2 has no type for representing a time without a date
   component.

3. DB-API is unclear about the required behavior of multiple calls to
   `Connection.cursor` on a connection.  Comdb2 does not have a concept of
   cursors as distinct from connection handles, so each time
   `Connection.cursor` is called, we call `Cursor.close` on any existing, open
   cursor for that connection.
"""

from __future__ import annotations

import functools
import itertools
import weakref
import datetime
import re

from . import cdb2
from .cdb2 import ColumnType, Row, Value, ParameterValue
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, List

__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "ColumnType",
    "Connection",
    "Cursor",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "Datetime",
    "DatetimeUs",
    "Binary",
    "Timestamp",
    "TimestampUs",
    "DatetimeFromTicks",
    "DatetimeUsFromTicks",
    "TimestampFromTicks",
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "InternalError",
    "OperationalError",
    "ProgrammingError",
    "IntegrityError",
    "DataError",
    "NotSupportedError",
    "UniqueKeyConstraintError",
    "ForeignKeyConstraintError",
    "NonNullConstraintError",
    "Value",
    "ParameterValue",
]

apilevel = "2.0"
"""This module conforms to the Python Database API Specification 2.0."""

threadsafety = 1
"""Two threads can use this module, but can't share one `Connection`."""

paramstyle = "pyformat"
"""The SQL placeholder format for this module is ``%(name)s``.

Comdb2's native placeholder format is ``@name``, but that cannot be used by
this module because it's not an acceptable `DB-API 2.0 placeholder style
<https://www.python.org/dev/peps/pep-0249/#paramstyle>`_.  This module uses
``pyformat`` because it is the only DB-API 2.0 paramstyle that we can translate
into Comdb2's placeholder format without needing a SQL parser.

Note:
    An int value is bound as ``%(my_int)s``, not as ``%(my_int)d`` - the last
    character is always ``s``.

Note:
    Because SQL strings for this module use the ``pyformat`` placeholder style,
    any literal ``%`` characters in a query must be escaped by doubling them.
    ``WHERE name like 'M%'`` becomes ``WHERE name LIKE 'M%%'``.
"""

_FIRST_WORD_OF_STMT = re.compile(
    r"""
    (?:           # match (without capturing)
      \s*         #   optional whitespace
      /\*.*?\*/   #   then a C-style /* ... */ comment, possibly across lines
    |             # or
      \s*         #   optional whitespace
      --[^\n]*\n  #   then a SQL-style comment terminated by a newline
    )*            # repeat until all comments have been matched
    \s*           # then skip over any whitespace
    (\w+)         # and capture the first word
    """,
    re.VERBOSE | re.DOTALL | re.ASCII,
)
_VALID_SP_NAME = re.compile(r"^[A-Za-z0-9_.]+$")


@functools.total_ordering
class _TypeObject:
    def __init__(self, *value_names):
        self.value_names = value_names
        self.values = [cdb2.TYPE[v] for v in value_names]

    def __eq__(self, other):
        return other in self.values

    def __lt__(self, other):
        return self != other and other < self.values

    def __repr__(self):
        return "TypeObject" + str(self.value_names)


def _binary(string: str | bytes) -> bytes:
    if isinstance(string, str):
        return string.encode("utf-8")
    return bytes(string)


STRING = _TypeObject("CSTRING")
"""The type codes of TEXT result columns compare equal to this constant."""

BINARY = _TypeObject("BLOB")
"""The type codes of BLOB result columns compare equal to this constant."""

NUMBER = _TypeObject("INTEGER", "REAL")
"""The type codes of numeric result columns compare equal to this constant."""

DATETIME = _TypeObject("DATETIME", "DATETIMEUS")
"""The type codes of datetime result columns compare equal to this constant."""

ROWID = STRING

# comdb2 doesn't support Date or Time, so I'm not defining them.
Datetime = datetime.datetime
DatetimeUs = cdb2.DatetimeUs
Binary = _binary
Timestamp = Datetime
TimestampUs = DatetimeUs

DatetimeFromTicks = Datetime.fromtimestamp
DatetimeUsFromTicks = DatetimeUs.fromtimestamp
TimestampFromTicks = Timestamp.fromtimestamp
TimestampUsFromTicks = TimestampUs.fromtimestamp

UserException = Exception


class Error(UserException):
    """This is the base class of all exceptions raised by this module.

    In addition to being available at the module scope, this class and the
    other exception classes derived from it are exposed as attributes on
    `Connection` objects, to simplify error handling in environments where
    multiple connections from different modules are used.
    """

    pass


class Warning(UserException):
    """Exception raised for important warnings.

    This is required to exist by the DB-API interface, but we never raise it.
    """

    pass


class InterfaceError(Error):
    """Exception raised for errors caused by misuse of this module."""

    pass


class DatabaseError(Error):
    """Base class for all errors reported by the database."""

    pass


class InternalError(DatabaseError):
    """Exception raised for internal errors reported by the database."""

    pass


class OperationalError(DatabaseError):
    """Exception raised for errors related to the database's operation.

    These errors are not necessarily the result of a bug either in the
    application or in the database - for example, dropped connections.
    """

    pass


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors reported by the database.

    For example, this will be raised for syntactically incorrect SQL, or for
    passing a different number of parameters than are required by the query.
    """

    pass


class IntegrityError(DatabaseError):
    """Exception raised for integrity errors reported by the database.

    For example, a subclass of this will be raised if a foreign key constraint
    would be violated, or a constraint that a column may not be null, or that
    an index may not have duplicates.  Other types of constraint violations may
    raise this type directly.
    """

    pass


class UniqueKeyConstraintError(IntegrityError):
    """Exception raised when a unique key constraint would be broken.

    Committing after either an INSERT or an UPDATE could result in this being
    raised, by either adding a new row that violates a unique (non-dup) key
    constraint or modifying an existing row in a way that would violate one.

    .. versionadded:: 1.1
    """

    pass


class ForeignKeyConstraintError(IntegrityError):
    """Exception raised when a foreign key constraint would be broken.

    This would be raised when committing if the changes being committed would
    violate referential integrity according to a foreign key constraint
    configured on the database.  For instance, deleting a row from a parent
    table would raise this if rows corresponding to its key still exist in
    a child table and the constraint doesn't have ON DELETE CASCADE.  Likewise,
    inserting a row into a child table would raise this if there was no row in
    the parent table matching the new row's key.

    .. versionadded:: 1.1
    """

    pass


class NonNullConstraintError(IntegrityError):
    """Exception raised when a non-null constraint would be broken.

    Committing after either an INSERT or an UPDATE could result in this being
    raised if it would result in a null being stored in a non-nullable column.
    Note that columns in a Comdb2 are not nullable by default.

    .. versionadded:: 1.1
    """

    pass


class DataError(DatabaseError):
    """Exception raised for errors related to the processed data.

    For example, this will be raised if you attempt to write a value that's out
    of range for the column type that would store it, or if you specify an
    invalid timezone name for the connection.
    """

    pass


class NotSupportedError(DatabaseError):
    """Exception raised when unsupported operations are attempted."""

    pass


_EXCEPTION_BY_RC = {
    cdb2.ERROR_CODE["CONNECT_ERROR"]: OperationalError,
    cdb2.ERROR_CODE["NOTCONNECTED"]: ProgrammingError,
    cdb2.ERROR_CODE["PREPARE_ERROR"]: ProgrammingError,
    cdb2.ERROR_CODE["IO_ERROR"]: OperationalError,
    cdb2.ERROR_CODE["INTERNAL"]: InternalError,
    cdb2.ERROR_CODE["NOSTATEMENT"]: ProgrammingError,
    cdb2.ERROR_CODE["BADCOLUMN"]: ProgrammingError,
    cdb2.ERROR_CODE["BADSTATE"]: ProgrammingError,
    cdb2.ERROR_CODE["ASYNCERR"]: OperationalError,
    cdb2.ERROR_CODE["INVALID_ID"]: InternalError,
    cdb2.ERROR_CODE["RECORD_OUT_OF_RANGE"]: OperationalError,
    cdb2.ERROR_CODE["REJECTED"]: OperationalError,
    cdb2.ERROR_CODE["STOPPED"]: OperationalError,
    cdb2.ERROR_CODE["BADREQ"]: OperationalError,
    cdb2.ERROR_CODE["DBCREATE_FAILED"]: OperationalError,
    cdb2.ERROR_CODE["THREADPOOL_INTERNAL"]: OperationalError,
    cdb2.ERROR_CODE["READONLY"]: NotSupportedError,
    cdb2.ERROR_CODE["NOMASTER"]: InternalError,
    cdb2.ERROR_CODE["UNTAGGED_DATABASE"]: NotSupportedError,
    cdb2.ERROR_CODE["CONSTRAINTS"]: IntegrityError,
    cdb2.ERROR_CODE["DEADLOCK"]: OperationalError,
    cdb2.ERROR_CODE["TRAN_IO_ERROR"]: OperationalError,
    cdb2.ERROR_CODE["ACCESS"]: OperationalError,
    cdb2.ERROR_CODE["TRAN_MODE_UNSUPPORTED"]: NotSupportedError,
    cdb2.ERROR_CODE["VERIFY_ERROR"]: OperationalError,
    cdb2.ERROR_CODE["FKEY_VIOLATION"]: ForeignKeyConstraintError,
    cdb2.ERROR_CODE["NULL_CONSTRAINT"]: NonNullConstraintError,
    cdb2.ERROR_CODE["CONV_FAIL"]: DataError,
    cdb2.ERROR_CODE["NONKLESS"]: NotSupportedError,
    cdb2.ERROR_CODE["MALLOC"]: OperationalError,
    cdb2.ERROR_CODE["NOTSUPPORTED"]: NotSupportedError,
    cdb2.ERROR_CODE["DUPLICATE"]: UniqueKeyConstraintError,
    cdb2.ERROR_CODE["TZNAME_FAIL"]: DataError,
    cdb2.ERROR_CODE["UNKNOWN"]: OperationalError,
}


def _raise_wrapped_exception(exc):
    code = exc.error_code
    msg = "%s (cdb2api rc %d)" % (exc.error_message, code)
    if "null constraint violation" in msg:
        raise NonNullConstraintError(msg) from exc  # DRQS 86013831
    raise _EXCEPTION_BY_RC.get(code, OperationalError)(msg) from exc


def _sql_operation(sql):
    match = _FIRST_WORD_OF_STMT.match(sql)
    if match:
        return match.group(1).lower()
    return None


def _operation_ends_transaction(operation):
    return operation == "commit" or operation == "rollback"


def _modifies_rows(operation):
    # These operations can modify the contents of the database.
    # exec is deliberately excluded because it might return a result set, and
    # this function is used to determine whether it's safe to call
    # cdb2_get_effects after running the operation.
    return operation in ("commit", "insert", "update", "delete")


def connect(
    database_name: str | bytes,
    tier: str | bytes = "default",
    autocommit: bool = False,
    host: str | bytes | None = None,
) -> Connection:
    """Establish a connection to a Comdb2 database.

    All arguments are passed directly through to the `Connection` constructor.

    Note:
        DB-API 2.0 requires the module to expose `connect`, but not
        `Connection`.  If portability across database modules is a concern, you
        should always use `connect` to create your connections rather than
        calling the `Connection` constructor directly.

    Returns:
        Connection: A handle for the newly established connection.
    """
    return Connection(
        database_name=database_name,
        tier=tier,
        autocommit=autocommit,
        host=host,
    )


class Connection:
    """Represents a connection to a Comdb2 database.

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

    The connection will use UTC as its timezone by default - you can change
    this with a ``SET TIMEZONE`` statement if needed.

    By default, or if ``autocommit`` is ``False``, `cursor` will return cursors
    that behave as mandated by the Python DB API: every statement to be
    executed is implicitly considered to be part of a transaction, and that
    transaction must be ended explicitly with a call to `commit` (or
    `rollback`).  If ``autocommit`` is ``True``, `cursor` will instead return
    cursors that behave more in line with Comdb2's traditional behavior: the
    side effects of any given statement are immediately committed unless you
    previously started a transaction by executing a ``begin`` statement.

    Note:
        Using ``autocommit=True`` will ease porting from code using other
        Comdb2 APIs, both because other Comdb2 APIs implicitly commit after
        each statement in the same way as an autocommit `Connection` will, and
        because there are certain operations that Comdb2 will implicitly
        retry when outside of a transaction but won't retry when inside
        a transaction - meaning that a non-autocommit `Connection` has extra
        failure modes.  You should strongly consider using ``autocommit=True``,
        especially for read-only use cases.

    Note:
        Python does not guarantee that object finalizers will be called when
        the interpreter exits, so to ensure that the connection is cleanly
        released you should call the `close` method when you're done with it.
        You can use `contextlib.closing` to guarantee the connection is
        released when a block completes.

    Note:
        DB-API 2.0 requires the module to expose `connect`, but not
        `Connection`.  If portability across database modules is a concern, you
        should always use `connect` to create your connections rather than
        instantiating this class directly.

    Args:
        database_name (str): The name of the database to connect to.
        tier (str): The cluster to connect to.
        host (str): Alternately, a single remote host to connect to.
        autocommit (bool): Whether to automatically commit after DML
            statements, disabling DB-API 2.0's automatic implicit transactions.
    """

    def __init__(
        self,
        database_name: str | bytes,
        tier: str | bytes = "default",
        autocommit: bool = False,
        host: str | bytes | None = None,
    ) -> None:
        if host is not None and tier != "default":
            raise InterfaceError(
                "Connecting to a host by name and to a "
                "cluster by tier are mutually exclusive"
            )

        self._active_cursor = None
        self._in_transaction = False
        self._autocommit = autocommit
        try:
            self._hndl = cdb2.Handle(database_name, tier, host=host)
        except cdb2.Error as e:
            _raise_wrapped_exception(e)

    def _check_closed(self):
        if self._hndl is None:
            raise InterfaceError("Attempted to use a closed Connection")

    @property
    def row_factory(self) -> Callable[[list[str]], Callable[[list[Value]], Row]]:
        """Factory used when constructing result rows.

        By default, or when set to ``None``, each row is returned as a `list`
        of column values.  If you'd prefer to receive rows as a `dict` or as
        a `collections.namedtuple`, you can set this property to one of the
        factories provided by the `comdb2.factories` module.

        Example:
            >>> from comdb2.factories import dict_row_factory
            >>> conn.row_factory = dict_row_factory
            >>> cursor = conn.cursor()
            >>> cursor.execute("SELECT 1 as 'foo', 2 as 'bar'")
            >>> cursor.fetchone()
            {'foo': 1, 'bar': 2}

        .. versionadded:: 0.9
        """
        self._check_closed()
        return self._hndl.row_factory

    @row_factory.setter
    def row_factory(
        self, value: Callable[[list[str]], Callable[[list[Value]], Row]]
    ) -> None:
        self._check_closed()
        self._hndl.row_factory = value

    def _close_any_outstanding_cursor(self):
        if self._active_cursor is not None:
            cursor = self._active_cursor()
            if cursor is not None and not cursor._closed:
                cursor.close()

    def _execute(self, operation):
        cursor = None
        if self._active_cursor is not None:
            cursor = self._active_cursor()
        if cursor is None:
            cursor = self.cursor()
        cursor._execute(operation, operation)

    def close(self, ack_current_event: bool = True) -> None:
        """Gracefully close the Comdb2 connection.

        Once a `Connection` has been closed, no further operations may be
        performed on it.

        If the connection was used to consume events from a `Lua consumer`__,
        then *ack_current_event* tells the database what to do with the
        last event that was delivered. By default it will be marked as consumed
        and won't be redelivered, but if ``ack_current_event=False`` then
        the event will be redelivered to another consumer for processing.

        __ https://bloomberg.github.io/comdb2/triggers.html#lua-consumers

        If a socket pool is running on the machine and the connection was in
        a clean state, this will turn over the connection to the socket pool.
        This cannot be done if the connection was in a transaction or
        in the middle of retrieving a result set.  Other restrictions may apply
        as well.

        You can ensure that this gets called at the end of a block using
        something like:

            >>> with contextlib.closing(connect('mattdb')) as conn:
            >>>     for row in conn.cursor().execute("select 1"):
            >>>         print(row)
        """
        if self._hndl is None:
            raise InterfaceError("close() called on already closed connection")
        self._close_any_outstanding_cursor()
        self._hndl.close(ack_current_event=ack_current_event)
        self._hndl = None

    def commit(self) -> None:
        """Commit any pending transaction to the database.

        This method will fail if the `Connection` is in ``autocommit`` mode and
        no transaction was explicitly started.
        """
        self._check_closed()
        self._execute("commit")

    def rollback(self) -> None:
        """Rollback the current transaction.

        This method will fail if the `Connection` is in ``autocommit`` mode and
        no transaction was explicitly started.

        Note:
            Closing a connection without committing the changes first will
            cause an implicit rollback to be performed, but will also prevent
            that connection from being contributed to the socket pool, if one
            is available.  Because of this, an explicit rollback should be
            preferred when possible.
        """
        self._check_closed()
        self._execute("rollback")

    def cursor(self) -> Cursor:
        """Return a new `Cursor` for this connection.

        This calls `Cursor.close` on any outstanding `Cursor`; only one
        `Cursor` is allowed per `Connection` at a time.

        Note:
            Although outstanding cursors are closed, uncommitted transactions
            started by them are not rolled back, so the new `Cursor` will begin
            in the middle of that uncommitted transaction.

        Returns:
            Cursor: A new cursor on this connection.
        """
        self._check_closed()
        self._close_any_outstanding_cursor()
        cursor = Cursor(self)
        self._active_cursor = weakref.ref(cursor)
        return cursor

    # Optional DB API Extension
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
    """Class used to send requests through a database connection.

    This class is not meant to be instantiated directly; it should always be
    created using `Connection.cursor`.  It provides methods for sending
    requests to the database and for reading back the result sets produced by
    the database.

    Queries are made using the `execute` and `callproc` methods.  Result sets
    can be consumed with the `fetchone`, `fetchmany`, or `fetchall` methods, or
    (as a nonstandard DB-API 2.0 extension) by iterating over the `Cursor`.

    Note:
        Only one `Cursor` per `Connection` can exist at a time.  Creating a new
        one will `close` the previous one.
    """

    _ErrorMessagesByOperation = {
        "begin": "Transactions may not be started explicitly",
        "commit": "Use Connection.commit to commit transactions",
        "rollback": "Use Connection.rollback to roll back transactions",
    }

    def __init__(self, conn: Connection) -> None:
        self._arraysize = 1
        self._conn = conn
        self._hndl = conn._hndl
        self._description = None
        self._closed = False
        self._rowcount = -1

    def _check_closed(self):
        if self._closed:
            raise InterfaceError("Attempted to use a closed cursor")

    @property
    def arraysize(self) -> int:
        """Controls the number of rows to fetch at a time with `fetchmany`.

        The default is ``1``, meaning that a single row will be fetched at
        a time.
        """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._arraysize = value

    @property
    def description(
        self,
    ) -> tuple[tuple[str, object, None, None, None, None, None], ...]:
        """Provides the name and type of each column in the latest result set.

        This read-only attribute will contain one element per column in the
        result set.  Each of those elements will be a 7-element sequence whose
        first element is the name of that column, whose second element is
        a type code for that column, and whose five remaining elements are
        ``None``.

        The type codes can be compared for equality against the `STRING`,
        `NUMBER`, `DATETIME`, and `BINARY` objects exposed by this module.  If
        you need more granularity (e.g. knowing whether the result is
        a ``REAL`` or an ``INTEGER``) you can compare the type code for
        equality against the members of the `.cdb2.TYPE` dictionary.  Or, of
        course, you can use `isinstance` to check the type of object returned
        as that column's value.

        Example:
            >>> cursor = connect('mattdb').cursor()
            >>> cursor.execute("select 1 as 'x', '2' as 'y', 3.0 as 'z'")
            >>> cursor.description[0][:2] == ('x', NUMBER)
            True
            >>> cursor.description[1][:2] == ('y', STRING)
            True
            >>> cursor.description[2][:2] == ('z', NUMBER)
            True
            >>> cursor.description[2][:2] == ('z', TYPE['INTEGER'])
            False
            >>> cursor.description[2][:2] == ('z', TYPE['REAL'])
            True
        """
        self._check_closed()
        return self._description

    @property
    def rowcount(self) -> int:
        """Provides the count of rows modified by the last transaction.

        For `Cursor` objects on a `Connection` that is not using ``autocommit``
        mode, this count is valid only after the transaction is committed with
        `Connection.commit()`.  For `Cursor` objects on a `Connection` that is
        using ``autocommit`` mode, this count is valid after a successful
        ``COMMIT``, or after an ``INSERT``, ``UPDATE``, or ``DELETE`` statement
        run outside of an explicit transaction.  At all other times, ``-1`` is
        returned.

        In particular, ``-1`` is returned whenever a transaction is in
        progress, because Comdb2 by default handles commit conflicts with other
        transactions by rerunning each statement of the transaction.  As
        a result, row counts obtained within a transaction are meaningless in
        the default transaction level; either more or fewer rows may be
        affected when the transaction eventually commits.

        Also, ``-1`` is returned after ``SELECT`` or ``SELECTV``, because
        querying the row count requires calling ``cdb2_get_effects``, which
        would consume the result set before the user could iterate over it.
        Likewise, ``-1`` is returned after ``EXEC PROCEDURE``, because a stored
        procedure could emit a result set.
        """
        self._check_closed()
        return self._rowcount

    # Optional DB API Extension
    @property
    def connection(self) -> Connection:
        """Return a reference to the `Connection` that this `Cursor` uses."""
        self._check_closed()
        return self._conn

    def close(self) -> None:
        """Close the cursor now.

        From this point forward an exception will be raised if any
        operation is attempted with this `Cursor`.

        Note:
            This does not rollback any uncommitted operations executed by this
            `Cursor`.  A new `Cursor` created from the `Connection` that this
            `Cursor` uses will start off in the middle of that uncommitted
            transaction.
        """
        self._check_closed()
        self._description = None
        self._closed = True

    def callproc(self, procname: str, parameters: Sequence[ParameterValue]) -> Sequence[ParameterValue]:
        """Call a stored procedure with the given name.

        The ``parameters`` sequence must contain one entry for each argument
        that the procedure requires.

        If the called procedure emits a result set, it is made available
        through the fetch methods, or by iterating over the `Cursor`, as though
        it was returned by a ``select`` statement.

        Args:
            procname (str): The name of the stored procedure to be executed.
            parameters (Sequence[Any]): A sequence of values to be passed, in
                order, as the parameters to the stored procedure.  Each element
                must be an instance of one of the Python types listed in
                :doc:`types`.

        Returns:
            List[Any]: A copy of the input parameters.
        """
        if not _VALID_SP_NAME.match(procname):
            raise NotSupportedError("Invalid procedure name '%s'" % procname)
        params_as_dict = {str(i): e for i, e in enumerate(parameters)}
        sql = (
            "exec procedure "
            + procname
            + "("
            + ", ".join("%%(%d)s" % i for i in range(len(params_as_dict)))
            + ")"
        )
        self.execute(sql, params_as_dict)
        return parameters[:]

    def execute(
        self,
        sql: str,
        parameters: Mapping[str, ParameterValue] | None = None,
        *,
        column_types: Sequence[ColumnType] | None = None,
    ) -> Cursor:
        """Execute a database operation (query or command).

        The ``sql`` string must be provided as a Python format string, with
        parameter placeholders represented as ``%(name)s`` and all other ``%``
        signs escaped as ``%%``.

        Note:
            Using placeholders should always be the preferred method of
            parameterizing the SQL query, as it prevents SQL injection
            vulnerabilities, and is faster than dynamically building SQL
            strings.

        If ``column_types`` is provided and non-empty, it must be a sequence of
        members of the `ColumnType` enumeration. The database will coerce the
        data in the Nth column of the result set to the Nth given column type.
        An error will be raised if the number of elements in ``column_types``
        doesn't match the number of columns in the result set, or if one of the
        elements is not a supported column type, or if coercion fails. If
        ``column_types`` is empty or not provided, no coercion is performed.

        Note:
            Databases APIs are not required to allow result set column types to
            be specified explicitly. We allow this as a non-standard DB-API 2.0
            extension.

        Args:
            sql (str): The SQL string to execute, as a Python format string.
            parameters (Mapping[str, Any]): An optional mapping from parameter
                names to the values to be bound for them.
            column_types (Sequence[int]): An optional sequence of types (values
                of the `ColumnType` enumeration) which the columns of the
                result set will be coerced to.

        Returns:
            Cursor: As a nonstandard DB-API 2.0 extension, this method returns
            the `Cursor` that it was called on, which can be used as an
            iterator over the result set returned by the query.  Iterating over
            it will yield one ``list`` per row in the result set, where the
            elements in the list correspond to the result columns within the
            row, in positional order.

            The `Connection.row_factory` property can be used to return rows as
            a different type.

        Example:
            >>> cursor.execute("select 1, 2 UNION ALL select %(x)s, %(y)s",
            ...                {'x': 2, 'y': 4})
            >>> cursor.fetchall()
            [[1, 2], [2, 4]]
        """
        self._check_closed()
        self._description = None
        operation = _sql_operation(sql)

        if not self._conn._autocommit:
            # Certain operations are forbidden when not in autocommit mode.
            errmsg = self._ErrorMessagesByOperation.get(operation)
            if errmsg:
                raise InterfaceError(errmsg)

        self._execute(operation, sql, parameters, column_types=column_types)
        if self._rowcount == -1:
            self._load_description()
        # Optional DB API Extension: execute's return value is unspecified.  We
        # return an iterable over the rows, but this isn't portable across DBs.
        return self

    def executemany(
        self, sql: str, seq_of_parameters: Sequence[Mapping[str, ParameterValue]]
    ) -> None:
        """Execute the same SQL statement repeatedly with different parameters.

        This is currently equivalent to calling execute multiple times, once
        for each element provided in ``seq_of_parameters``.

        Args:
            sql (str): The SQL string to execute, as a Python format string of
                the format expected by `execute`.
            seq_of_parameters (Sequence[Mapping[str, Any]]): A sequence of
                mappings from parameter names to the values to be bound for
                them.  The ``sql`` statement will be run once per element in
                this sequence.
        """
        self._check_closed()
        for parameters in seq_of_parameters:
            self.execute(sql, parameters)

    def _execute(self, operation, sql, parameters=None, *, column_types=None):
        self._rowcount = -1

        if not self._conn._autocommit:
            # Any non-SET operation starts a txn when not in autocommit mode.
            if not self._conn._in_transaction and operation != "set":
                try:
                    self._hndl.execute("begin")
                except cdb2.Error as e:
                    _raise_wrapped_exception(e)
                self._conn._in_transaction = True

        if parameters is None:
            parameters = {}

        try:
            # If variable interpolation fails, then translate the exception to
            # an InterfaceError to signal that it's a client-side problem.
            sql = sql % {name: "@" + name for name in parameters}
        except KeyError as keyerr:
            msg = "No value provided for parameter %s" % keyerr
            raise InterfaceError(msg) from keyerr
        except Exception as exc:
            msg = "Invalid Python format string for query"
            raise InterfaceError(msg) from exc

        if _operation_ends_transaction(operation):
            self._conn._in_transaction = False  # txn ends, even on failure

        try:
            self._hndl.execute(sql, parameters, column_types=column_types)
        except cdb2.Error as e:
            _raise_wrapped_exception(e)

        if operation == "begin":
            self._conn._in_transaction = True  # txn successfully started
        elif not self._conn._in_transaction and _modifies_rows(operation):
            # We're not in a transaction, and the last statement could have
            # modified rows.  Either we've just explicitly committed
            # a transaction, or we're in autocommit mode and ran DML outside of
            # an explicit transaction.  We can get the count of affected rows.
            self._update_rowcount()

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        """No-op; implemented for PEP-249 compliance."""
        self._check_closed()

    def setoutputsize(self, size: Any, column: int = None) -> None:
        """No-op; implemented for PEP-249 compliance."""
        self._check_closed()

    def _update_rowcount(self):
        try:
            self._rowcount = self._hndl.get_effects()[0]
        except cdb2.Error:
            self._rowcount = -1

    def _load_description(self):
        names = self._hndl.column_names()
        types = self._hndl.column_types()
        self._description = tuple(
            (name, type, None, None, None, None, None)
            for name, type in zip(names, types)
        )
        if not self._description:
            self._description = None

    def fetchone(self) -> Row | None:
        """Fetch the next row of the current result set.

        Returns:
            If no rows remain in the current result set, ``None`` is
            returned, otherwise the next row of the result set is returned.  By
            default the row is returned as a `list`, where the elements in the
            list correspond to the result row's columns in positional order,
            but this can be changed with the `Connection.row_factory` property.
        """
        try:
            return next(self)
        except StopIteration:
            return None

    def fetchmany(self, n: int | None = None) -> List[Row]:
        """Fetch the next set of rows of the current result set.

        Args:
            n: Maximum number of rows to be returned.  If this argument is not
                given, `Cursor.arraysize` is used as the maximum.

        Returns:
            list: Returns a `list` containing the next ``n`` rows of the
            result set.  If fewer than ``n`` rows remain, the returned list
            will contain fewer than ``n`` elements.  If no rows remain, the
            list will be empty.  By default each row is
            returned as a `list`, where the elements in the list correspond to
            the result row's columns in positional order, but this can be
            changed with the `Connection.row_factory` property.
        """
        if n is None:
            n = self._arraysize
        return [x for x in itertools.islice(self, 0, n)]

    def fetchall(self) -> List[Row]:
        """Fetch all remaining rows of the current result set.

        Returns:
            list: Returns a `list` containing all remaining rows of the
            result set.  By default each row is returned as a `list`, where the
            elements in the list correspond to the result row's columns in
            positional order, but this can be changed with the
            `Connection.row_factory` property.
        """
        return [x for x in self]

    # Optional DB API Extension
    def __iter__(self) -> Iterator[Row]:
        """Iterate over all rows in a result set.

        By default each row is returned as a `list`, where the elements in the
        list correspond to the result row's columns in positional order, but
        this can be changed with the `Connection.row_factory` property.

        Note:
            This is not required by DB-API 2.0; for maximum portability
            applications should prefer to use `fetchone` or `fetchmany` or
            `fetchall` instead.

        Example:
            >>> cursor.execute("select 1, 2 UNION ALL select 3, 4")
            >>> for row in cursor:
            ...     print(row)
            [1, 2]
            [3, 4]
        """
        self._check_closed()
        return self

    # Optional DB API Extension
    def next(self):
        self._check_closed()
        if not self._description:
            raise InterfaceError("No result set exists")
        try:
            return next(self._hndl)
        except cdb2.Error as e:
            _raise_wrapped_exception(e)

    __next__ = next
