from __future__ import absolute_import, unicode_literals

import functools
import itertools
import weakref
import datetime
import re
import six

from . import cdb2

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

_FIRST_WORD_OF_LINE = re.compile(r'(\S+)')
_VALID_SP_NAME = re.compile(r'^[A-Za-z0-9_.]+$')


@functools.total_ordering
class _TypeObject(object):
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values

    def __lt__(self, other):
        return self != other and other < self.values


def _binary(string):
    if isinstance(string, six.text_type):
        return string.encode('utf-8')
    return bytes(string)

STRING = _TypeObject(cdb2.TYPE['CSTRING'])
BINARY = _TypeObject(cdb2.TYPE['BLOB'])
NUMBER = _TypeObject(cdb2.TYPE['INTEGER'], cdb2.TYPE['REAL'])
DATETIME = _TypeObject(cdb2.TYPE['DATETIME'], cdb2.TYPE['DATETIMEUS'])
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
    cdb2.ERROR_CODE['CONNECT_ERROR']         : OperationalError,
    cdb2.ERROR_CODE['NOTCONNECTED']          : ProgrammingError,
    cdb2.ERROR_CODE['PREPARE_ERROR']         : ProgrammingError,
    cdb2.ERROR_CODE['IO_ERROR']              : OperationalError,
    cdb2.ERROR_CODE['INTERNAL']              : InternalError,
    cdb2.ERROR_CODE['NOSTATEMENT']           : ProgrammingError,
    cdb2.ERROR_CODE['BADCOLUMN']             : ProgrammingError,
    cdb2.ERROR_CODE['BADSTATE']              : ProgrammingError,
    cdb2.ERROR_CODE['ASYNCERR']              : OperationalError,

    cdb2.ERROR_CODE['INVALID_ID']            : InternalError,
    cdb2.ERROR_CODE['RECORD_OUT_OF_RANGE']   : OperationalError,

    cdb2.ERROR_CODE['REJECTED']              : OperationalError,
    cdb2.ERROR_CODE['STOPPED']               : OperationalError,
    cdb2.ERROR_CODE['BADREQ']                : OperationalError,
    cdb2.ERROR_CODE['DBCREATE_FAILED']       : OperationalError,

    cdb2.ERROR_CODE['THREADPOOL_INTERNAL']   : OperationalError,
    cdb2.ERROR_CODE['READONLY']              : NotSupportedError,

    cdb2.ERROR_CODE['NOMASTER']              : InternalError,
    cdb2.ERROR_CODE['UNTAGGED_DATABASE']     : NotSupportedError,
    cdb2.ERROR_CODE['CONSTRAINTS']           : IntegrityError,
    cdb2.ERROR_CODE['DEADLOCK']              : OperationalError,

    cdb2.ERROR_CODE['TRAN_IO_ERROR']         : OperationalError,
    cdb2.ERROR_CODE['ACCESS']                : OperationalError,

    cdb2.ERROR_CODE['TRAN_MODE_UNSUPPORTED'] : NotSupportedError,

    cdb2.ERROR_CODE['VERIFY_ERROR']          : OperationalError,
    cdb2.ERROR_CODE['FKEY_VIOLATION']        : IntegrityError,
    cdb2.ERROR_CODE['NULL_CONSTRAINT']       : IntegrityError,

    cdb2.ERROR_CODE['CONV_FAIL']             : DataError,
    cdb2.ERROR_CODE['NONKLESS']              : NotSupportedError,
    cdb2.ERROR_CODE['MALLOC']                : OperationalError,
    cdb2.ERROR_CODE['NOTSUPPORTED']          : NotSupportedError,

    cdb2.ERROR_CODE['DUPLICATE']             : IntegrityError,
    cdb2.ERROR_CODE['TZNAME_FAIL']           : DataError,

    cdb2.ERROR_CODE['UNKNOWN']               : OperationalError,
}


def _raise_wrapped_exception(exc):
    code = exc.error_code
    msg = exc.error_message
    if "null constraint violation" in msg:
        raise IntegrityError(msg)  # DRQS 86013831
    raise _EXCEPTION_BY_RC.get(code, OperationalError)(msg)


def _sql_operation(sql):
    match = _FIRST_WORD_OF_LINE.search(sql)
    if match:
        return match.group(1).lower()
    return None


def _operation_ends_transaction(operation):
    return operation == 'commit' or operation == 'rollback'


def _modifies_rows(operation):
    # These operations can modify the contents of the database.
    # exec is deliberately excluded because it might return a result set, and
    # this function is used to determine whether it's safe to call
    # cdb2_get_effects after running the operation.
    return operation in ('commit', 'insert', 'update', 'delete')


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


class Connection(object):
    def __init__(self, database_name, tier="default", autocommit=False):
        self._active_cursor = None
        self._in_transaction = False
        self._autocommit = autocommit
        try:
            self._hndl = cdb2.Handle(database_name, tier)
        except cdb2.Error as e:
            _raise_wrapped_exception(e)

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

    def close(self):
        if self._hndl is None:
            raise InterfaceError("close() called on already closed connection")
        self._close_any_outstanding_cursor()
        self._hndl.close()
        self._hndl = None

    def commit(self):
        self._execute("commit")

    def rollback(self):
        self._execute("rollback")

    def cursor(self):
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


class Cursor(object):
    ErrorMessagesByOperation = {
        'begin': "Transactions may not be started explicitly",
        'commit': "Use Connection.commit to commit transactions",
        'rollback': "Use Connection.rollback to roll back transactions",
    }

    def __init__(self, conn):
        self.arraysize = 1
        self._conn = conn
        self._hndl = conn._hndl
        self._description = None
        self._closed = False
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

    # Optional DB API Extension
    @property
    def connection(self):
        self._check_closed()
        return self._conn

    def close(self):
        self._check_closed()
        self._description = None
        self._closed = True

    def callproc(self, procname, parameters):
        if not _VALID_SP_NAME.match(procname):
            raise NotSupportedError("Invalid procedure name '%s'" % procname)
        params_as_dict = {str(i): e for i, e in enumerate(parameters)}
        sql = ("exec procedure " + procname + "("
              + ", ".join("%%(%d)s" % i for i in range(len(params_as_dict)))
              + ")")
        self.execute(sql, params_as_dict)
        return parameters[:]

    def execute(self, sql, parameters=None):
        self._check_closed()
        self._description = None
        operation = _sql_operation(sql)

        if not self._conn._autocommit:
            # Certain operations are forbidden when not in autocommit mode.
            errmsg = self.ErrorMessagesByOperation.get(operation)
            if errmsg:
                raise InterfaceError(errmsg)

        self._execute(operation, sql, parameters)
        self._load_description()
        # Optional DB API Extension: execute's return value is unspecified.  We
        # return an iterable over the rows, but this isn't portable across DBs.
        return self

    def executemany(self, sql, seq_of_parameters):
        self._check_closed()
        for parameters in seq_of_parameters:
            self.execute(sql, parameters)

    def _execute(self, operation, sql, parameters=None):
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

        sql = sql % {name: "@" + name for name in parameters}

        if _operation_ends_transaction(operation):
            self._conn._in_transaction = False  # txn ends, even on failure

        try:
            self._hndl.execute(sql, parameters)
        except cdb2.Error as e:
            _raise_wrapped_exception(e)

        if operation == 'begin':
            self._conn._in_transaction = True  # txn successfully started
        elif not self._conn._in_transaction and _modifies_rows(operation):
            # We're not in a transaction, and the last statement could have
            # modified rows.  Either we've just explicitly committed
            # a transaction, or we're in autocommit mode and ran DML outside of
            # an explicit transaction.  We can get the count of affected rows.
            self._update_rowcount()

    def setinputsizes(self, sizes):
        self._check_closed()

    def setoutputsize(self, size, column=None):
        self._check_closed()

    def _update_rowcount(self):
        try:
            self._rowcount = self._hndl.get_effects()[0]
        except cdb2.Error:
            self._rowcount = -1

    def _load_description(self):
        names = self._hndl.column_names()
        types = self._hndl.column_types()
        self._description = tuple((name, type, None, None, None, None, None)
                                  for name, type in zip(names, types))
        if not self._description:
            self._description = None

    def fetchone(self):
        try:
            return next(self)
        except StopIteration:
            return None

    def fetchmany(self, n=None):
        if n is None:
            n = self.arraysize
        return [x for x in itertools.islice(self, 0, n)]

    def fetchall(self):
        return [x for x in self]

    # Optional DB API Extension
    def __iter__(self):
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
