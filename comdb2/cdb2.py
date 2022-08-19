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
blob           `six.binary_type` (aka `bytes` in Python 3, ``str`` in Python 2)
text           `six.text_type` (aka `str` in Python 3, ``unicode`` in Python 2)
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
from __future__ import absolute_import, unicode_literals

from ._cdb2_types import Error, Effects, DatetimeUs
from ._ccdb2 import Handle as CHandle

__all__ = ['Error', 'Handle', 'Effects', 'DatetimeUs',
           'ERROR_CODE', 'TYPE', 'HANDLE_FLAGS']

# Pull all comdb2 error codes from cdb2api.h into our namespace
ERROR_CODE = {u'CONNECT_ERROR'         : -1,
              u'NOTCONNECTED'          : -2,
              u'PREPARE_ERROR'         : -3,
              u'IO_ERROR'              : -4,
              u'INTERNAL'              : -5,
              u'NOSTATEMENT'           : -6,
              u'BADCOLUMN'             : -7,
              u'BADSTATE'              : -8,
              u'ASYNCERR'              : -9,
              u'INVALID_ID'            : -12,
              u'RECORD_OUT_OF_RANGE'   : -13,
              u'REJECTED'              : -15,
              u'STOPPED'               : -16,
              u'BADREQ'                : -17,
              u'DBCREATE_FAILED'       : -18,
              u'THREADPOOL_INTERNAL'   : -20,
              u'READONLY'              : -21,
              u'NOMASTER'              : -101,
              u'UNTAGGED_DATABASE'     : -102,
              u'CONSTRAINTS'           : -103,
              u'DEADLOCK'              : 203,
              u'TRAN_IO_ERROR'         : -105,
              u'ACCESS'                : -106,
              u'TRAN_MODE_UNSUPPORTED' : -107,
              u'VERIFY_ERROR'          : 2,
              u'FKEY_VIOLATION'        : 3,
              u'NULL_CONSTRAINT'       : 4,
              u'CONV_FAIL'             : 113,
              u'NONKLESS'              : 114,
              u'MALLOC'                : 115,
              u'NOTSUPPORTED'          : 116,
              u'DUPLICATE'             : 299,
              u'TZNAME_FAIL'           : 401,
              u'UNKNOWN'               : 300,
             }
"""This dict maps all known Comdb2 error names to their respective values.

The value returned in `Error.error_code` will generally be the value
corresponding to one of the keys in this dict, though that's not always
guaranteed because new error codes can be added to the Comdb2 server at any
time.
"""

# Pull comdb2 column types from cdb2api.h into our namespace
TYPE = {u'INTEGER'      : 1,
        u'REAL'         : 2,
        u'CSTRING'      : 3,
        u'BLOB'         : 4,
        u'DATETIME'     : 6,
        u'INTERVALYM'   : 7,
        u'INTERVALDS'   : 8,
        u'DATETIMEUS'   : 9,
        u'INTERVALDSUS' : 10,
       }
"""This dict maps all known Comdb2 types to their enumeration value.

Each value in the list returned by `Handle.column_types` will generally be the
value corresponding to one of the keys in this dict, though that's not always
guaranteed because new types can be added to the Comdb2 server at any time.
"""

# Pull comdb2 handle flags from cdb2api.h into our namespace
HANDLE_FLAGS = {u'READ_INTRANS_RESULTS' : 2,
                u'DIRECT_CPU'           : 4,
                u'RANDOM'               : 8,
                u'RANDOMROOM'           : 16,
                u'ROOM'                 : 32,
               }
"""This dict maps all known Comdb2 flags to their enumeration value.

These values can be passed directly to `Handle`, though values not in this dict
can be passed as well (such as the bitwise OR of two different flags).
"""


class Handle(object):
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

    def __init__(self, database_name, tier="default", flags=0, tz='UTC',
                 host=None):
        if host is not None:
            if tier != "default":
                raise Error(ERROR_CODE['NOTSUPPORTED'],
                            "Connecting to a host by name and to a "
                            "cluster by tier are mutually exclusive")
            else:
                tier = host
                flags |= HANDLE_FLAGS['DIRECT_CPU']

        self._hndl = CHandle(database_name, tier, flags)
        if tz is not None:
            self._hndl.execute("set timezone %s" % tz, {})
        self._cursor = iter([])

    def close(self, ack_current_event=True):
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
    def row_factory(self):
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
    def row_factory(self, value):
        self._hndl.row_factory = value

    def execute(self, sql, parameters=None):
        """Execute a database operation (query or command).

        The ``sql`` string may have placeholders for parameters to be passed.
        This should always be the preferred method of parameterizing the SQL
        query, as it prevents SQL injection vulnerabilities and is faster.
        Placeholders for named parameters must be in Comdb2's native format,
        ``@param_name``.

        Args:
            sql (str): The SQL string to execute.
            parameters (Mapping[str, Any]): An optional mapping from parameter
                names to the values to be bound for them.

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
        self._cursor = iter(self._hndl.execute(sql, parameters))
        return self

    def __iter__(self):
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

    def get_effects(self):
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

    def column_names(self):
        """Returns the names of the columns of the current result set.

        Returns:
            A list of unicode strings, one per column in the result set.
        """
        return self._hndl.column_names()

    def column_types(self):
        """Returns the type codes of the columns of the current result set.

        Returns:
            List[int]: A list of integers, one per column in the result set.
            Each generally corresponds to one of the types in the `TYPE` global
            object exposed by this module.
        """
        return self._hndl.column_types()
