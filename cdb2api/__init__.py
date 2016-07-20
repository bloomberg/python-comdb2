import itertools
import datetime
import re

from ._cdb2api import lib
import cdb2api

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
DatetimeUs = cdb2api.DatetimeUs
Binary = cdb2api.Binary
Timestamp = Datetime
TimestampUs = DatetimeUs

DatetimeFromTicks = Datetime.fromtimestamp
DatetimeUsFromTicks = cdb2api.DatetimeUs.fromtimestamp
TimestampFromTicks = DatetimeFromTicks

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
    cdb2api.ERROR_CODE['CONNECT_ERROR']         : OperationalError,
    cdb2api.ERROR_CODE['NOTCONNECTED']          : ProgrammingError,
    cdb2api.ERROR_CODE['PREPARE_ERROR']         : ProgrammingError,
    cdb2api.ERROR_CODE['IO_ERROR']              : OperationalError,
    cdb2api.ERROR_CODE['INTERNAL']              : InternalError,
    cdb2api.ERROR_CODE['NOSTATEMENT']           : ProgrammingError,
    cdb2api.ERROR_CODE['BADCOLUMN']             : ProgrammingError,
    cdb2api.ERROR_CODE['BADSTATE']              : ProgrammingError,
    cdb2api.ERROR_CODE['ASYNCERR']              : OperationalError,

    cdb2api.ERROR_CODE['INVALID_ID']            : InternalError,
    cdb2api.ERROR_CODE['RECORD_OUT_OF_RANGE']   : OperationalError,

    cdb2api.ERROR_CODE['REJECTED']              : OperationalError,
    cdb2api.ERROR_CODE['STOPPED']               : OperationalError,
    cdb2api.ERROR_CODE['BADREQ']                : OperationalError,
    cdb2api.ERROR_CODE['DBCREATE_FAILED']       : OperationalError,

    cdb2api.ERROR_CODE['THREADPOOL_INTERNAL']   : OperationalError,
    cdb2api.ERROR_CODE['READONLY']              : NotSupportedError,

    cdb2api.ERROR_CODE['NOMASTER']              : InternalError,
    cdb2api.ERROR_CODE['UNTAGGED_DATABASE']     : NotSupportedError,
    cdb2api.ERROR_CODE['CONSTRAINTS']           : IntegrityError,
    cdb2api.ERROR_CODE['DEADLOCK']              : OperationalError,

    cdb2api.ERROR_CODE['TRAN_IO_ERROR']         : OperationalError,
    cdb2api.ERROR_CODE['ACCESS']                : OperationalError,

    cdb2api.ERROR_CODE['TRAN_MODE_UNSUPPORTED'] : NotSupportedError,

    cdb2api.ERROR_CODE['VERIFY_ERROR']          : OperationalError,
    cdb2api.ERROR_CODE['FKEY_VIOLATION']        : IntegrityError,
    cdb2api.ERROR_CODE['NULL_CONSTRAINT']       : IntegrityError,

    cdb2api.ERROR_CODE['CONV_FAIL']             : DataError,
    cdb2api.ERROR_CODE['NONKLESS']              : NotSupportedError,
    cdb2api.ERROR_CODE['MALLOC']                : OperationalError,
    cdb2api.ERROR_CODE['NOTSUPPORTED']          : NotSupportedError,

    cdb2api.ERROR_CODE['DUPLICATE']             : IntegrityError,
    cdb2api.ERROR_CODE['TZNAME_FAIL']           : DataError,

    cdb2api.ERROR_CODE['UNKNOWN']               : OperationalError,

    # XXX This is returned for duplicates, despite not being an error code...
    1                                           : IntegrityError,
}


def _raise_wrapped_exception(exc):
    code = exc.error_code
    msg = exc.error_message
    if "null constraint violation" in msg:
        raise IntegrityError(msg)  # DRQS 86013831
    raise _EXCEPTION_BY_RC.get(code, OperationalError)(msg)


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


class Connection(object):
    def __init__(self, database_name, tier="default"):
        self._active_cursor = None
        try:
            self._hndl = cdb2api.Handle(database_name, tier)
        except cdb2api.Error as e:
            _raise_wrapped_exception(e)

    def __del__(self):
        if self._hndl is not None:
            self.close()

    def close(self):
        if self._hndl is None:
            raise InterfaceError("close() called on already closed connection")
        if self._active_cursor is not None:
            if not self._active_cursor._closed:
                self._active_cursor.close()
        self._hndl.close()
        self._hndl = None

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
        self._rowcount = -1

        if not self._in_transaction and not _SET.match(sql):
            try:
                self._hndl.execute("begin")
            except cdb2api.Error as e:
                _raise_wrapped_exception(e)
            self._in_transaction = True

        if parameters is not None:
            sql = sql % {name: "@" + name for name in parameters}

        if sql == 'commit' or sql == 'rollback':
            self._in_transaction = False

        try:
            self._hndl.execute(sql, parameters)
        except cdb2api.Error as e:
            _raise_wrapped_exception(e)

        if sql == 'commit':
            self._update_rowcount()

        self._load_description()

    def setinputsizes(self, sizes):
        self._check_closed()

    def setoutputsize(self, size, column=None):
        self._check_closed()

    def _update_rowcount(self):
        try:
            self._rowcount = self._hndl.get_effects()[0]
        except cdb2api.Error:
            self._rowcount = -1

    def _load_description(self):
        names = self._hndl.column_names()
        types = self._hndl.column_types()
        self._description = tuple((name, type, None, None, None, None, None)
                                  for name, type in zip(names, types))

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

    def __iter__(self):
        self._check_closed()
        return self._hndl

    def next(self):
        try:
            return next(self._hndl)
        except cdb2api.Error as e:
            _raise_wrapped_exception(e)

    __next__ = next
