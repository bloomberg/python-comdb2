from __future__ import absolute_import, unicode_literals

from ._cdb2api import ffi, lib
from datetime import datetime, timedelta
from collections import namedtuple
import pytz
import six

__all__ = ['Error', 'Handle', 'DatetimeUs',
           'ERROR_CODE', 'TYPE', 'HANDLE_FLAGS']

# Pull all comdb2 error codes from cdb2api.h into our namespace
ERROR_CODE = {six.text_type(k[len('CDB2ERR_'):]): v
              for k, v in ffi.typeof('enum cdb2_errors').relements.items()
              if k.startswith('CDB2ERR_')}

# Pull comdb2 column types from cdb2api.h into our namespace
TYPE = {six.text_type(k[len('CDB2_'):]): v
        for k, v in ffi.typeof('enum cdb2_coltype').relements.items()
        if k.startswith('CDB2_')}

# Pull comdb2 handle flags from cdb2api.h into our namespace
HANDLE_FLAGS = {
        six.text_type(k[len('CDB2_'):]): v
        for k, v in ffi.typeof('enum cdb2_hndl_alloc_flags').relements.items()
        if k.startswith('CDB2_')}

Effects = namedtuple('Effects',
    "num_affected num_selected num_updated num_deleted num_inserted")


class DatetimeUs(datetime):
    """DatetimeUs objects give microsecond precision when bound as parameters.

    This differs from datetime.datetime parameters, which only give millisecond
    precision.  The behavior of DatetimeUs is otherwise identical to
    datetime.datetime, with the exception of a single extra classmethod,
    fromdatetime, for constructing a DatetimeUs from a datetime.datetime.
    """
    @classmethod
    def fromdatetime(cls, dt):
        """Returns a DatetimeUs copied from a given datetime.datetime"""
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
        error_code: The integer error code from the failed cdb2api call.
        error_message: The string returned by cdb2api's cdb2_errstr after
            the failed call.
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
    errstr = _ffi_string(lib.cdb2_errstr(hndl))
    if not isinstance(errstr, str):
        errstr = errstr.decode('utf-8')  # bytes to str for Python 3
    return errstr


def _check_rc(rc, hndl):
    if rc != 0:
        errstr = _errstr(hndl)
        raise Error(rc, errstr)


def _cdb2_client_datetime_common(val, ptr):
    struct_time = val.timetuple()
    ptr.tm.tm_sec = struct_time.tm_sec
    ptr.tm.tm_min = struct_time.tm_min
    ptr.tm.tm_hour = struct_time.tm_hour
    ptr.tm.tm_mday = struct_time.tm_mday
    ptr.tm.tm_mon = struct_time.tm_mon - 1
    ptr.tm.tm_year = struct_time.tm_year - 1900
    ptr.tm.tm_wday = struct_time.tm_wday
    ptr.tm.tm_yday = struct_time.tm_yday - 1
    ptr.tm.tm_isdst = struct_time.tm_isdst
    if val.tzname() is not None:
        tzname = val.tzname()
        if not isinstance(tzname, bytes):
            tzname = tzname.encode('utf-8')
        ptr.tzname = tzname


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
    """Low level Pythonic wrapper around Comdb2 connections using cdb2api."""

    def __init__(self, database_name, tier="default", flags=0, tz='UTC',
                 host=None):
        """Creates a new connection to a given database.

        By default, the connection will be made to the cluster configured as
        the machine-wide default for the given database.  This is almost always
        what you want.  If you need to connect to a database that's running on
        your local machine rather than a cluster, you can pass "local" as the
        tier.  It's also permitted to specify "dev", "alpha", "beta", or "prod"
        as the tier, but note that the firewall will block you from connecting
        directly from a dev machine to a prod database.

        Alternately, you can pass a machine name as the host argument, to
        connect directly to an instance of the given database on that host,
        rather than on a cluster or the local machine.

        By default, the connection will use UTC as its timezone.  This differs
        from cdb2api's default behavior, where the timezone used by the query
        differs depending on the machine that it is run from.  If for some
        reason you need to have that machine-specific default timezone instead,
        you can pass None for the tz argument.  Any other valid timezone name
        may also be used instead of 'UTC'.

        Note that Python does not guarantee that object finalizers will be
        called when the interpreter exits, so to ensure that the handle is
        cleanly released you should call the close() method when you're done
        with it.  You can use contextlib.closing to guarantee the handle is
        released when a block completes.

        Args:
            database_name: The name of the database to connect to.
            tier: The cluster to connect to.
            host: Alternately, a single remote host to connect to.
            flags: An integer flags value passed directly through to cdb2_open.
            tz: The timezone to be used by the new connection, or None to use
                a machine-specific default.
        """
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

        Once a Handle has been closed, no further operations may be performed
        on it.

        If a socket pool is running on the machine and the Handle was in
        a clean state, this will turn over the connection to the socket pool.
        This can only be done if the Handle was not in a transaction, nor in
        the middle of a result set.  Other restrictions may apply as well.

        You can ensure that this gets called at the end of a block using
        something like:

            with contextlib.closing(Handle('mattdb')) as hndl:
                for row in hndl.execute("select 1"):
                    print row
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

        By default, or when set to None, rows are returned as lists of column
        values.  If you'd prefer to receive rows as a dict or as a namedtuple,
        you can set this property to one of the factories provided by the
        `comdb2.factories` module.
        """
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._row_factory = value

    def execute(self, sql, parameters=None):
        """Execute a SQL query, returning an iterator over its result set.

        The provided SQL query may have placeholders for parameters to be
        passed.  This should always be the prefered method of parameterizing
        the SQL query, as it prevents SQL injection vulnerabilities and is
        faster.  Placeholders for named parameters must be in Comdb2's native
        format, @param_name.

        Args:
            sql: The SQL string to execute.
            parameters: An optional mapping from parameter names to the values
                to be bound for them.

        Returns:
            An iterator over the result set returned by the query.  The
            iterator will yield one list per row in the result set, where the
            elements in the list correspond to the result columns within the
            row, in positional order.

            The `row_factory` property can be used to return rows as
            a different type, instead.

        Example:
            >>> for row in hndl.execute('SELECT 1, 2 UNION select @x, @y',
            ...                         {'x': 2, 'y': 4}):
            ...     print row[1], row[0]
            4 2
            2 1

            >>> from comdb2.factories import dict_row_factory
            >>> hndl.row_factory = dict_row_factory
            >>> for row in hndl.execute("SELECT 1 as 'foo', 2 as 'bar'"):
            ...     print row
            {'foo': 1, 'bar': 2}
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
            six.raise_from(Error(lib.CDB2ERR_UNKNOWN, str(e)), e)
        return self

    def __iter__(self):
        return self

    def __next__(self):
        if not self._more_rows_available:
            raise StopIteration()

        try:
            data = [self._column_value(i) for i in self._column_range]
            self._next_record()
        except UnicodeDecodeError as e:
            # Allow _consume_all_rows to raise.  Its error code is relevant,
            # since it may indicate that the connection is no longer usable.
            self._consume_all_rows()
            # If it didn't, raise our own error for the failed UTF-8 decode.
            six.raise_from(Error(lib.CDB2ERR_CONV_FAIL, str(e)), e)

        if self._row_class is not None:
            try:
                data = self._row_class(data)
            except Exception as e:
                six.raise_from(Error(lib.CDB2ERR_UNKNOWN, str(e)), e)

        return data

    next = __next__

    def get_effects(self):
        """Return counts of rows affected by executed queries.

        Within a transaction, these counts are a running total from the start
        of the transaction up through the last executed SQL statement.  Outside
        of a transaction, these counts represent the rows affected by only the
        last executed SQL statement.

        Note that the results within a transaction are not necessarily reliable
        unless the VERIFYRETRY setting is turned off.  All of the caveats of
        the cdb2_get_effects call apply.

        Returns:
            tuple: A 5-tuple, whose elements are:
                The total number of rows that were affected
                The number of rows that were selected
                The number of rows that were updated
                The number of rows that were deleted
                The number of rows that were inserted
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
            A list of strings, one per column in the result set.
        """
        return [_ffi_string(lib.cdb2_column_name(self._hndl, i))
                for i in self._column_range]

    def column_types(self):
        """Returns the type codes of the columns of the current result set.

        Returns:
            A list of integers, one per column in the result set.  Each
            corresponds to one of the types in the TYPE global object exposed
            by this module.
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
        raise Error(lib.CDB2ERR_NOTSUPPORTED,
                    "Can't handle type %d returned by database!" % typecode)
