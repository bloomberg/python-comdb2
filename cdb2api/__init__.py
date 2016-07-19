from __future__ import print_function

import itertools
import datetime
import pytz
import six
import re

from ._cdb2api import ffi, lib

__all__ = ['apilevel', 'threadsafety', 'paramstyle',
           'connect', 'Connection', 'Cursor',
           'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID',
           'Datetime', 'DatetimeUs', 'Binary', 'Timestamp', 'TimestampUs',
           'DatetimeFromTicks', 'DatetimeUsFromTicks', 'TimestampFromTicks',
           'Error', 'Warning', 'InterfaceError', 'DatabaseError',
           'InternalError', 'OperationalError', 'ProgrammingError',
           'IntegrityError', 'DataError', 'NotSupportedError']

apilevel = "2.0"
threadsafety = 1  # 2 threads can have different connections, but can't share 1
paramstyle = "pyformat"

_SET = re.compile(r'^\s*set', re.I)
_TXN = re.compile(r'^\s*(begin|commit|rollback)', re.I)


def _construct_datetime(cls, tm, microseconds, tzname):
    return cls(year=tm.tm_year + 1900,
               month=tm.tm_mon + 1,
               day=tm.tm_mday,
               hour=tm.tm_hour,
               minute=tm.tm_min,
               second=tm.tm_sec,
               microsecond=microseconds,
               tzinfo=pytz.timezone(ffi.string(tzname)))


def _datetime(ptr):
    return _construct_datetime(Datetime, ptr.tm, ptr.msec * 1000, ptr.tzname)


def _datetimeus(ptr):
    return _construct_datetime(DatetimeUs, ptr.tm, ptr.usec, ptr.tzname)


def _comdb2_to_py(typecode, val, size):
    if val == ffi.NULL:
        return None
    if typecode == lib.CDB2_INTEGER:
        return ffi.cast("int64_t *", val)[0]
    if typecode == lib.CDB2_REAL:
        return ffi.cast("double *", val)[0]
    if typecode == lib.CDB2_BLOB:
        return Binary(ffi.buffer(val, size)[:])
    if typecode == lib.CDB2_CSTRING:
        return ffi.string(ffi.cast("char *", val))
    if typecode == lib.CDB2_DATETIMEUS:
        return _datetimeus(ffi.cast("cdb2_client_datetimeus_t *", val))
    if typecode == lib.CDB2_DATETIME:
        return _datetime(ffi.cast("cdb2_client_datetime_t *", val))
    raise NotSupportedError("Can't handle type %d returned by database!"
                            % typecode)


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
    ptr.tzname = val.tzname()


def _cdb2_client_datetime_t(val):
    ptr = ffi.new("cdb2_client_datetime_t *")
    _cdb2_client_datetime_common(val, ptr)
    ptr.msec = val.microsecond // 1000
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
        return lib.CDB2_INTEGER, ffi.new("int64_t *", val), 8
    elif isinstance(val, float):
        return lib.CDB2_REAL, ffi.new("double *", val), 8
    elif isinstance(val, Binary):
        return lib.CDB2_BLOB, bytes(val), len(val)
    elif isinstance(val, bytes):
        return lib.CDB2_CSTRING, val, len(val)
    elif isinstance(val, DatetimeUs):
        return (lib.CDB2_DATETIMEUS, _cdb2_client_datetimeus_t(val),
                ffi.sizeof("cdb2_client_datetimeus_t"))
    elif isinstance(val, Datetime):
        return (lib.CDB2_DATETIME, _cdb2_client_datetime_t(val),
                ffi.sizeof("cdb2_client_datetime_t"))
    else:
        raise InterfaceError("Can't map type %s to a comdb2 type"
                             % val.__class__.__name__)


class _TypeObject(object):
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

STRING = _TypeObject(lib.CDB2_CSTRING)
BINARY = _TypeObject(lib.CDB2_BLOB)
NUMBER = _TypeObject(lib.CDB2_INTEGER, lib.CDB2_REAL)
DATETIME = _TypeObject(lib.CDB2_DATETIME, lib.CDB2_DATETIMEUS)
ROWID = STRING

# comdb2 doesn't support Date or Time, so I'm not defining them.
Datetime = datetime.datetime


class DatetimeUs(datetime.datetime):
    '''DatetimeUs parameters to Cursor.execute will give microsecond precision.

    This differs from datetime.datetime parameters, which only give millisecond
    precision.  The behavior is otherwise identical to datetime.datetime.'''
    def __add__(self, other):
        ret = super(DatetimeUs, self).__add__(other)
        return self.combine(ret.date(), ret.timetz())

    def __sub__(self, other):
        ret = super(DatetimeUs, self).__sub__(other)
        return self.combine(ret.date(), ret.timetz())

    @classmethod
    def now(cls, tz=None):
        ret = super(DatetimeUs, cls).now(tz)
        return cls.combine(ret.date(), ret.timetz())

    @classmethod
    def fromtimestamp(cls, timestamp, tz=None):
        ret = super(DatetimeUs, cls).fromtimestamp(timestamp, tz)
        return cls.combine(ret.date(), ret.timetz())

    def astimezone(self, tz):
        ret = super(DatetimeUs, self).astimezone(tz)
        return self.combine(ret.date(), ret.timetz())


DatetimeFromTicks = Datetime.fromtimestamp
DatetimeUsFromTicks = DatetimeUs.fromtimestamp
TimestampFromTicks = DatetimeFromTicks
Binary = type('Binary', (bytes,), {})
Timestamp = Datetime
TimestampUs = DatetimeUs

try:
    UserException = StandardError  # Python 2
except NameError:
    UserException = Exception      # Python 3


class Error(UserException):
    pass


class Warning(UserException):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


_EXCEPTION_BY_RC = {
    lib.CDB2ERR_CONNECT_ERROR         : OperationalError,
    lib.CDB2ERR_NOTCONNECTED          : ProgrammingError,
    lib.CDB2ERR_PREPARE_ERROR         : ProgrammingError,
    lib.CDB2ERR_IO_ERROR              : OperationalError,
    lib.CDB2ERR_INTERNAL              : InternalError,
    lib.CDB2ERR_NOSTATEMENT           : ProgrammingError,
    lib.CDB2ERR_BADCOLUMN             : ProgrammingError,
    lib.CDB2ERR_BADSTATE              : ProgrammingError,
    lib.CDB2ERR_ASYNCERR              : OperationalError,

    lib.CDB2ERR_INVALID_ID            : InternalError,
    lib.CDB2ERR_RECORD_OUT_OF_RANGE   : OperationalError,

    lib.CDB2ERR_REJECTED              : OperationalError,
    lib.CDB2ERR_STOPPED               : OperationalError,
    lib.CDB2ERR_BADREQ                : OperationalError,
    lib.CDB2ERR_DBCREATE_FAILED       : OperationalError,

    lib.CDB2ERR_THREADPOOL_INTERNAL   : OperationalError,
    lib.CDB2ERR_READONLY              : NotSupportedError,

    lib.CDB2ERR_NOMASTER              : InternalError,
    lib.CDB2ERR_UNTAGGED_DATABASE     : NotSupportedError,
    lib.CDB2ERR_CONSTRAINTS           : IntegrityError,
    lib.CDB2ERR_DEADLOCK              : OperationalError,

    lib.CDB2ERR_TRAN_IO_ERROR         : OperationalError,
    lib.CDB2ERR_ACCESS                : OperationalError,

    lib.CDB2ERR_TRAN_MODE_UNSUPPORTED : NotSupportedError,

    lib.CDB2ERR_VERIFY_ERROR          : OperationalError,
    lib.CDB2ERR_FKEY_VIOLATION        : IntegrityError,
    lib.CDB2ERR_NULL_CONSTRAINT       : IntegrityError,
    lib.CDB2_OK_DONE                  : IntegrityError,

    lib.CDB2ERR_CONV_FAIL             : DataError,
    lib.CDB2ERR_NONKLESS              : NotSupportedError,
    lib.CDB2ERR_MALLOC                : OperationalError,
    lib.CDB2ERR_NOTSUPPORTED          : NotSupportedError,

    lib.CDB2ERR_DUPLICATE             : IntegrityError,
    lib.CDB2ERR_TZNAME_FAIL           : DataError,

    lib.CDB2ERR_UNKNOWN               : OperationalError,
}


def _errstr(hndl):
    errstr = ffi.string(lib.cdb2_errstr(hndl))
    if not isinstance(errstr, str):
        errstr = errstr.decode('utf-8')  # bytes to str for Python 3
    return errstr


def _exception_for_rc(rc):
    return _EXCEPTION_BY_RC.get(rc, OperationalError)


def _check_rc(rc, hndl):
    if rc != lib.CDB2_OK:
        errstr = _errstr(hndl)
        if "null constraint violation" in errstr:
            raise IntegrityError(errstr)  # DRQS 86013831
        raise _exception_for_rc(rc)(errstr)


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


class Connection(object):
    def __init__(self, database_name, tier="default"):
        self._active_cursor = None
        self._hndl_p = None
        self._hndl = None

        self._hndl_p = ffi.new("struct cdb2_hndl **")
        rc = lib.cdb2_open(self._hndl_p, database_name, tier, 0)
        if rc != lib.CDB2_OK:
            errstr = _errstr(self._hndl_p[0])
            lib.cdb2_close(self._hndl_p[0])
            raise _exception_for_rc(rc)(errstr)

        self._hndl = self._hndl_p[0]

    def __del__(self):
        if self._hndl is not None:
            self.close()

    def close(self):
        if self._hndl is None:
            raise InterfaceError("close() called on already closed connection")
        if self._active_cursor is not None:
            if not self._active_cursor._closed:
                self._active_cursor.close()
        lib.cdb2_close(self._hndl)
        self._hndl = None
        self._hndl_p = None

    def commit(self):
        if self._active_cursor is not None:  # Else no SQL was ever executed
            self._active_cursor._execute("commit")

    def rollback(self):
        if self._active_cursor is not None:  # Else no SQL was ever executed
            self._active_cursor._execute("rollback")

    def cursor(self):
        if self._active_cursor is not None:
            if not self._active_cursor._closed:
                self._active_cursor.close()
        self._active_cursor = Cursor(self._hndl)
        return self._active_cursor


class Cursor(object):
    def __init__(self, hndl):
        self.arraysize = 1
        self._hndl = hndl
        self._description = None
        self._closed = False
        self._in_transaction = False
        self._more_rows_available = False
        self._rowcount = -1

    def _check_closed(self):
        if self._closed:
            raise InterfaceError("Attempted to use a closed cursor")

    @property
    def description(self):
        self._check_closed()
        return self._description

    @property
    def rowcount(self):
        self._check_closed()
        return self._rowcount

    def _consume_all_rows(self):
        while self._more_rows_available:
            self._next_record()

    def close(self):
        self._check_closed()
        if self._in_transaction:
            try:
                self._execute("rollback")
            except DatabaseError:
                # It's not useful to raise an exception if gracefully
                # terminating the session fails.
                pass
            self._in_transaction = False
        self._consume_all_rows()
        self._description = None
        self._closed = True

    def execute(self, sql, parameters=None):
        self._check_closed()
        if _TXN.match(sql):
            raise InterfaceError("Transaction control SQL statements can only"
                                 " be used on autocommit connections.")
        return self._execute(sql, parameters)

    def executemany(self, sql, seq_of_parameters):
        self._check_closed()
        for parameters in seq_of_parameters:
            self.execute(sql, parameters)

    def _execute(self, sql, parameters=None):
        self._consume_all_rows()
        self._rowcount = -1

        if not self._in_transaction and not _SET.match(sql):
            rc = lib.cdb2_run_statement(self._hndl, "begin")
            _check_rc(rc, self._hndl)
            self._in_transaction = True

        try:
            if parameters is not None:
                sql = sql % {name: "@" + name for name in parameters}
                params_cdata = self._bind_params(parameters)  # NOQA
                # XXX params_cdata owns memory that has been bound to cdb2api,
                # so must not be garbage collected before cdb2_run_statement

            if sql == 'commit' or sql == 'rollback':
                self._in_transaction = False

            rc = lib.cdb2_run_statement(self._hndl, sql)

            if sql == 'commit':
                self._update_rowcount()
        finally:
            lib.cdb2_clearbindings(self._hndl)

        _check_rc(rc, self._hndl)
        self._next_record()
        self._load_description()

    def _bind_params(self, parameters):
        params_cdata = []
        for key, val in parameters.items():
            if not isinstance(key, bytes):
                key = key.encode('utf-8')  # str to bytes for Python 3

            typecode, ptr, size = _bind_args(val)

            params_cdata.append(ptr)
            rc = lib.cdb2_bind_param(self._hndl, key, typecode, ptr, size)
            _check_rc(rc, self._hndl)

        return params_cdata

    def setinputsizes(self, sizes):
        self._check_closed()

    def setoutputsize(self, size, column=None):
        self._check_closed()

    def _next_record(self):
        self._more_rows_available = False
        rc = lib.cdb2_next_record(self._hndl)
        if rc == lib.CDB2_OK:
            self._more_rows_available = True
        elif rc != lib.CDB2_OK_DONE:
            _check_rc(rc, self._hndl)

    def _update_rowcount(self):
        effects = ffi.new("cdb2_effects_tp *")
        rc = lib.cdb2_get_effects(self._hndl, effects)
        if rc == lib.CDB2_OK:
            self._rowcount = effects.num_affected

    def _num_columns(self):
        return lib.cdb2_numcolumns(self._hndl)

    def _column_name(self, i):
        return lib.cdb2_column_name(self._hndl, i)

    def _column_type(self, i):
        return lib.cdb2_column_type(self._hndl, i)

    def _load_description(self):
        self._description = []

        for i in range(self._num_columns()):
            name = ffi.string(self._column_name(i))
            type_ = self._column_type(i)

            self._description.append((name, type_, None, None, None, None, None))

    def fetchone(self):
        self._check_closed()
        if not self._description:
            raise InterfaceError("No result set exists")
        try:
            return next(self)
        except StopIteration:
            return None

    def fetchmany(self, n=None):
        self._check_closed()
        if not self._description:
            raise InterfaceError("No result set exists")
        if n is None:
            n = self.arraysize
        return [x for x in itertools.islice(self, 0, n)]

    def fetchall(self):
        self._check_closed()
        if not self._description:
            raise InterfaceError("No result set exists")
        return [x for x in self]

    def _load_row(self):
        return tuple(self._load_column(i, d) for i, d in enumerate(self._description))

    def _load_column(self, i, description):
        val = lib.cdb2_column_value(self._hndl, i)
        size = lib.cdb2_column_size(self._hndl, i)
        typecode = description[1]
        return _comdb2_to_py(typecode, val, size)

    def __iter__(self):
        self._check_closed()
        return self

    def next(self):
        self._check_closed()
        if not self._more_rows_available:
            raise StopIteration()

        data = self._load_row()
        self._next_record()

        return data

    __next__ = next
