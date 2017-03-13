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

from ._cdb2api import ffi, lib
from datetime import datetime, timedelta
from collections import namedtuple
import pytz
import six

__all__ = ['Error', 'Handle', 'Effects', 'DatetimeUs',
           'ERROR_CODE', 'TYPE', 'HANDLE_FLAGS']

# Pull all comdb2 error codes from cdb2api.h into our namespace
ERROR_CODE = {six.text_type(k[len('CDB2ERR_'):]): v
              for k, v in ffi.typeof('enum cdb2_errors').relements.items()
              if k.startswith('CDB2ERR_')}
"""This dict maps all known Comdb2 error names to their respective values.

The value returned in `Error.error_code` will generally be the value
corresponding to one of the keys in this dict, though that's not always
guaranteed because new error codes can be added to the Comdb2 server at any
time.
"""

# Pull comdb2 column types from cdb2api.h into our namespace
TYPE = {six.text_type(k[len('CDB2_'):]): v
        for k, v in ffi.typeof('enum cdb2_coltype').relements.items()
        if k.startswith('CDB2_')}
"""This dict maps all known Comdb2 types to their enumeration value.

Each value in the list returned by `Handle.column_types` will generally be the
value corresponding to one of the keys in this dict, though that's not always
guaranteed because new types can be added to the Comdb2 server at any time.
"""

# Pull comdb2 handle flags from cdb2api.h into our namespace
HANDLE_FLAGS = {
        six.text_type(k[len('CDB2_'):]): v
        for k, v in ffi.typeof('enum cdb2_hndl_alloc_flags').relements.items()
        if k.startswith('CDB2_')}
"""This dict maps all known Comdb2 flags to their enumeration value.

These values can be passed directly to `Handle`, though values not in this dict
can be passed as well (such as the bitwise OR of two different flags).
"""


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
        return DatetimeUs(dt.year, dt.month, dt.day,
                          dt.hour, dt.minute, dt.second, dt.microsecond,
                          dt.tzinfo)

    def __add__(self, other):
        ret = super(DatetimeUs, self).__add__(other)
        if isinstance(ret, datetime):
            return DatetimeUs.fromdatetime(ret)
        return ret  # must be a timedelta

    def __sub__(self, other):
        ret = super(DatetimeUs, self).__sub__(other)
        if isinstance(ret, datetime):
            return DatetimeUs.fromdatetime(ret)
        return ret  # must be a timedelta

    def __radd__(self, other):
        return self + other

    @classmethod
    def now(cls, tz=None):
        ret = super(DatetimeUs, cls).now(tz)
        return DatetimeUs.fromdatetime(ret)

    @classmethod
    def fromtimestamp(cls, timestamp, tz=None):
        ret = super(DatetimeUs, cls).fromtimestamp(timestamp, tz)
        return DatetimeUs.fromdatetime(ret)

    def astimezone(self, tz):
        ret = super(DatetimeUs, self).astimezone(tz)
        return DatetimeUs.fromdatetime(ret)


class Error(RuntimeError):
    """Exception type raised for all failed operations.

    Attributes:
        error_code (int): The error code from the failed cdb2api call.
        error_message (str): The string returned by cdb2api's ``cdb2_errstr``
            after the failed call.
    """
    def __init__(self, error_code, error_message):
        self.error_code = error_code
        self.error_message = error_message
        super(Error, self).__init__(error_code, error_message)


def _ffi_string(cdata):
    return ffi.string(cdata).decode('utf-8')


def _construct_datetime(cls, tm, microseconds, tzname):
    timezone = pytz.timezone(_ffi_string(tzname))
    timestamp = cls(year=tm.tm_year + 1900,
                    month=tm.tm_mon + 1,
                    day=tm.tm_mday,
                    hour=tm.tm_hour,
                    minute=tm.tm_min,
                    second=tm.tm_sec,
                    microsecond=microseconds)
    return timezone.localize(timestamp, is_dst=tm.tm_isdst)


def _datetime(ptr):
    return _construct_datetime(datetime, ptr.tm, ptr.msec * 1000, ptr.tzname)


def _datetimeus(ptr):
    return _construct_datetime(DatetimeUs, ptr.tm, ptr.usec, ptr.tzname)


def _errstr(hndl):
    msg = ffi.string(lib.cdb2_errstr(hndl))
    try:
        errstr = msg.decode('utf-8')
    except UnicodeDecodeError:
        # The DB's error strings aren't necessarily UTF-8.
        # If one isn't, it's preferable to mangle the error string than to
        # raise a UnicodeDecodeError (which would obscure the root cause).
        # Return a unicode string with \x escapes in place of non-ascii bytes.
        errstr = msg.decode('latin1').encode('unicode_escape').decode('ascii')

    return errstr


def _check_rc(rc, hndl):
    if rc != 0:
        errstr = _errstr(hndl)
        raise Error(rc, errstr)


def _cdb2_client_datetime_common(val, ptr):
    struct_time = val.utctimetuple()
    ptr.tm.tm_sec = struct_time.tm_sec
    ptr.tm.tm_min = struct_time.tm_min
    ptr.tm.tm_hour = struct_time.tm_hour
    ptr.tm.tm_mday = struct_time.tm_mday
    ptr.tm.tm_mon = struct_time.tm_mon - 1
    ptr.tm.tm_year = struct_time.tm_year - 1900
    ptr.tm.tm_wday = struct_time.tm_wday
    ptr.tm.tm_yday = struct_time.tm_yday - 1
    ptr.tm.tm_isdst = struct_time.tm_isdst
    ptr.tzname = b'UTC'


def _cdb2_client_datetime_t(val):
    ptr = ffi.new("cdb2_client_datetime_t *")
    val += timedelta(microseconds=500)  # For rounding to nearest millisecond
    _cdb2_client_datetime_common(val, ptr)
    ptr.msec = val.microsecond // 1000  # For rounding to nearest millisecond
    return ptr


def _cdb2_client_datetimeus_t(val):
    ptr = ffi.new("cdb2_client_datetimeus_t *")
    _cdb2_client_datetime_common(val, ptr)
    ptr.usec = val.microsecond
    return ptr


def _bind_args(val):
    if val is None:
        return lib.CDB2_INTEGER, ffi.NULL, 0
    elif isinstance(val, six.integer_types):
        try:
            return lib.CDB2_INTEGER, ffi.new("int64_t *", val), 8
        except OverflowError as e:
            six.raise_from(Error(lib.CDB2ERR_CONV_FAIL,
                                 "Can't bind value %s: %s" % (val, e)), e)
    elif isinstance(val, float):
        return lib.CDB2_REAL, ffi.new("double *", val), 8
    elif isinstance(val, bytes):
        return lib.CDB2_BLOB, val, len(val)
    elif isinstance(val, six.text_type):
        val = val.encode('utf-8')
        return lib.CDB2_CSTRING, val, len(val)
    elif isinstance(val, DatetimeUs):
        return (lib.CDB2_DATETIMEUS, _cdb2_client_datetimeus_t(val),
                ffi.sizeof("cdb2_client_datetimeus_t"))
    elif isinstance(val, datetime):
        return (lib.CDB2_DATETIME, _cdb2_client_datetime_t(val),
                ffi.sizeof("cdb2_client_datetime_t"))
    raise Error(lib.CDB2ERR_NOTSUPPORTED,
                "Can't map type %s to a comdb2 type" % val.__class__.__name__)


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
                raise Error(lib.CDB2ERR_NOTSUPPORTED,
                            "Connecting to a host by name and to a "
                            "cluster by tier are mutually exclusive")
            else:
                tier = host
                flags |= HANDLE_FLAGS['DIRECT_CPU']

        self._more_rows_available = False
        self._row_factory = None
        self._hndl_p = None
        self._hndl = None
        self._lib_cdb2_close = lib.cdb2_close  # DRQS 88746293

        if not isinstance(database_name, bytes):
            database_name = database_name.encode('utf-8')  # Python 3

        if not isinstance(tier, bytes):
            tier = tier.encode('utf-8')  # Python 3

        self._hndl_p = ffi.new("struct cdb2_hndl **")
        rc = lib.cdb2_open(self._hndl_p, database_name, tier, flags)
        if rc != lib.CDB2_OK:
            errstr = _errstr(self._hndl_p[0])
            lib.cdb2_close(self._hndl_p[0])
            raise Error(rc, errstr)

        self._hndl = self._hndl_p[0]
        self._column_range = []

        if tz is not None:
            # XXX This is technically SQL injectable, but
            # a) SET statements don't go through a normal SQL parser anyway,
            # b) DRQS 86887068 leaves us no choice.
            self.execute("set timezone %s" % tz)

    def __del__(self):
        if self._hndl is not None:
            self._lib_cdb2_close(self._hndl)

    def close(self):
        """Gracefully close the Comdb2 connection.

        Once a `Handle` has been closed, no further operations may be performed
        on it.

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
        self._more_rows_available = False
        lib.cdb2_close(self._hndl)
        self._hndl = None
        self._hndl_p = None

        def closed_error(func_name):
            raise Error(lib.CDB2ERR_NOTCONNECTED,
                        "%s() called on closed connection" % func_name)

        # FIXME message always says column_types
        for func in ("close", "execute",
                     "get_effects", "column_names", "column_types"):
            setattr(self, func, lambda *a, **k: closed_error(func))

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
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._row_factory = value

    def execute(self, sql, parameters=None):
        """Execute a database operation (query or command).

        The ``sql`` string may have placeholders for parameters to be passed.
        This should always be the preferred method of parameterizing the SQL
        query, as it prevents SQL injection vulnerabilities and is faster.
        Placeholders for named parameters must be in Comdb2's native format,
        ``@param_name``.

        Args:
            sql (str): The SQL string to execute.
            parameters (Mapping[str, T]): An optional mapping from parameter
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
        self._column_range = []
        self._consume_all_rows()

        if not isinstance(sql, bytes):
            sql = sql.encode('utf-8')  # Python 3

        try:
            if parameters is not None:
                params_cdata = self._bind_params(parameters)  # NOQA
                # XXX params_cdata owns memory that has been bound to cdb2api,
                # so must not be garbage collected before cdb2_run_statement
            rc = lib.cdb2_run_statement(self._hndl, sql)
        finally:
            lib.cdb2_clearbindings(self._hndl)

        _check_rc(rc, self._hndl)
        self._more_rows_available = True
        self._next_record()
        self._column_range = range(lib.cdb2_numcolumns(self._hndl))
        try:
            if self._row_factory is None:
                self._row_class = None
            else:
                self._row_class = self._row_factory(self.column_names())
        except Exception as e:
            six.raise_from(Error(lib.CDB2ERR_UNKNOWN, six.text_type(e)), e)
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
        return self

    def __next__(self):
        if not self._more_rows_available:
            raise StopIteration()

        data = [self._column_value(i) for i in self._column_range]
        self._next_record()

        if self._row_class is not None:
            try:
                data = self._row_class(data)
            except Exception as e:
                six.raise_from(Error(lib.CDB2ERR_UNKNOWN, six.text_type(e)), e)

        return data

    next = __next__

    def get_effects(self):
        """Return counts of rows affected by executed queries.

        Within a transaction, these counts are a running total from the start
        of the transaction up through the last executed SQL statement.  Outside
        of a transaction, these counts represent the rows affected by only the
        last executed SQL statement.

        Note:
            The results within a transaction are not necessarily reliable
            unless the ``VERIFYRETRY`` setting is turned off.  All of the
            caveats of the ``cdb2_get_effects`` call apply.

        Returns:
            Effects: An count of rows that have been affected, selected,
            updated, deleted, or inserted.
        """
        effects = ffi.new("cdb2_effects_tp *")
        self._more_rows_available = False
        # XXX cdb2_get_effects consumes any remaining rows implicitly
        rc = lib.cdb2_get_effects(self._hndl, effects)
        _check_rc(rc, self._hndl)
        return Effects(effects.num_affected,
                       effects.num_selected,
                       effects.num_updated,
                       effects.num_deleted,
                       effects.num_inserted)

    def column_names(self):
        """Returns the names of the columns of the current result set.

        Returns:
            List[str]: A list of strings, one per column in the result set.
        """
        return [_ffi_string(lib.cdb2_column_name(self._hndl, i))
                for i in self._column_range]

    def column_types(self):
        """Returns the type codes of the columns of the current result set.

        Returns:
            List[int]: A list of integers, one per column in the result set.
            Each generally corresponds to one of the types in the `TYPE` global
            object exposed by this module.
        """
        return [lib.cdb2_column_type(self._hndl, i)
                for i in self._column_range]

    def _consume_all_rows(self):
        while self._more_rows_available:
            self._next_record()

    def _bind_params(self, parameters):
        params_cdata = []
        for key, val in parameters.items():
            if not isinstance(key, bytes):
                key = key.encode('utf-8')  # str to bytes for Python 3

            typecode, ptr, size = _bind_args(val)

            params_cdata.append((key, ptr))
            rc = lib.cdb2_bind_param(self._hndl, key, typecode, ptr, size)
            _check_rc(rc, self._hndl)

        return params_cdata

    def _next_record(self):
        try:
            rc = lib.cdb2_next_record(self._hndl)
        except:
            self._more_rows_available = False
            raise
        else:
            if rc != 0:
                self._more_rows_available = False
                if rc != lib.CDB2_OK_DONE:
                    _check_rc(rc, self._hndl)

    def _column_value(self, i):
        val = lib.cdb2_column_value(self._hndl, i)
        typecode = lib.cdb2_column_type(self._hndl, i)

        try:
            if val == ffi.NULL:
                return None
            if typecode == lib.CDB2_INTEGER:
                return lib.integer_value(val)
            if typecode == lib.CDB2_REAL:
                return lib.real_value(val)
            if typecode == lib.CDB2_BLOB:
                size = lib.cdb2_column_size(self._hndl, i)
                return bytes(ffi.buffer(val, size))
            if typecode == lib.CDB2_CSTRING:
                size = lib.cdb2_column_size(self._hndl, i)
                return six.text_type(ffi.buffer(val, size - 1), "utf-8")
            if typecode == lib.CDB2_DATETIMEUS:
                return _datetimeus(lib.datetimeus_value(val))
            if typecode == lib.CDB2_DATETIME:
                return _datetime(lib.datetime_value(val))
        except Exception as e:
            from_exception = e
            error_code = lib.CDB2ERR_CONV_FAIL
            why = six.text_type(from_exception)
        else:
            from_exception = None
            error_code = lib.CDB2ERR_NOTSUPPORTED
            why = "Unsupported column type"

        try:
            typename = ffi.typeof('enum cdb2_coltype').elements[typecode]
        except KeyError:
            typename = "type %d" % typecode

        col_name = self.column_names()[i]

        to_raise = Error(error_code,
                         "Failed to decode %s column %d ('%s'): %s" % (
                             typename, i, col_name, why))

        if from_exception is not None:
            six.raise_from(to_raise, from_exception)
        raise to_raise
