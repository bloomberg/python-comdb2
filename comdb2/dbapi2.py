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
    """This is the base class of all exceptions raised by this module.

    In addition to being available at the module scope, this class and the
    other exception classes derived from it are exposed as attributes on
    Connection objects, to simplify error handling in environments where
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

    For example, this will be raised if a foreign key constraint is violated,
    or a constraint that a column may not be null, or that an index may not
    have duplicates.
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
    """Establish a connection to a Comdb2 database.

    All arguments are passed directly through to the Connection constructor.

    Returns:
        A Connection object representing the newly established connection.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    def __init__(self, database_name, tier="default", autocommit=False):
        """Establish a connection to a Comdb2 database.

        By default, the connection will be made to the cluster configured as
        the machine-wide default for the given database.  This is almost always
        what you want.  If you need to connect to a database that's running on
        your local machine rather than a cluster, you can pass "local" as the
        tier.  It's also permitted to specify "dev", "alpha", "beta", or "prod"
        as the tier, but note that the firewall will block you from connecting
        directly from a dev machine to a prod database.

        If the autocommit keyword argument is left at its default value of
        False, cursors created from this Connection will behave as mandated by
        the Python DB API: every statement to be executed is implicitly
        considered to be part of a transaction, and that transaction must be
        ended explicitly with a call to Connection.commit() (or
        Connection.rollback()).  If, instead, the autocommit keyword argument
        is passed as True, cursors created from this Connection will behave
        more in line with Comdb2's traditional behavior: the side effects of
        any given statement are immediately committed unless you explicitly
        begin a transaction by executing a "begin" statement.  Note that using
        autocommit=True will ease porting from code using SqlService, both
        because SqlService implicitly committed after each statement in the
        same way as an autocommit Connection will, and because there are
        certain operations that a Comdb2 will implicitly retry outside of
        a transaction but won't retry inside a transaction - meaning that
        non-autocommit Connections have new failure modes.

        The connection will use UTC as its timezone.

        Note that Python does not guarantee that object finalizers will be
        called when the interpreter exits, so to ensure that the connection is
        cleanly released you should call the close() method when you're done
        with it.  You can use contextlib.closing to guarantee the connection is
        released when a block completes.

        Args:
            database_name: The name of the database to connect to.
            tier: The cluster to connect to.
            autocommit: Whether to automatically commit after DML statements.
        """
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
        """Gracefully close the Comdb2 connection.

        Once a Connection has been closed, no further operations may be performed
        on it.

        If a socket pool is running on the machine and the Connection was in
        a clean state, this will turn over the connection to the socket pool.
        This can only be done if the Connection was not in a transaction, nor in
        the middle of a result set.  Other restrictions may apply as well.
        """
        if self._hndl is None:
            raise InterfaceError("close() called on already closed connection")
        self._close_any_outstanding_cursor()
        self._hndl.close()
        self._hndl = None

    def commit(self):
        """Commit any pending transaction to the database."""
        self._execute("commit")

    def rollback(self):
        """Rollback to the start of any pending transaction to the database.

        Closing a connection without committing the changes first will cause an
        implicit rollback to be performed, but will also prevent the underlying
        connection from being contributed to the socket pool, if one is
        available.
        """
        self._execute("rollback")

    def cursor(self):
        """Return a new Cursor object using this connection.

        Note that this invalidates any outstanding cursors; only one
        outstanding cursor is allowed per connection.  Note also that although
        outstanding cursors are invalidated any uncommitted transactions
        started by them are not rolled back, so the new cursor will begin in
        the middle of that uncommitted transaction.

        Returns:
            A new cursor on this connection.
        """
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
        """Provides the name and type of each column in the latest result set.

        This read-only attribute will contain one element per column in the
        result set.  Each of those elements will be a 7-item sequence whose
        first item is the name of that column, whose second item is a type code
        for that column, and whose five remaining items are None.

        The type codes can be compared for equality against the STRING, NUMBER,
        DATETIME, and BINARY objects exposed by this module.  If you need more
        granularity (e.g. knowing whether the result is a REAL or an INTEGER)
        you can compare the type code for equality against the members of the
        TYPE dictionary exposed by the cdb2 module.  Or, of course, you can
        just do an isinstance() check against the object returned as that
        column's value.
        """
        self._check_closed()
        return self._description

    @property
    def rowcount(self):
        """Provides the count of rows modified by the last transaction.

        For cursors that are not using autocommit mode, this count is updated
        only after the transaction is committed with `Connection.commit()`.
        For cursors that are using autocommit mode, this count is updated after
        a successful COMMIT, or after an INSERT, UPDATE, or DELETE statement
        run outside of an explicit transaction.

        We don't update the rowcount property while we're still in
        a transaction, because Comdb2 by default handles commit conflicts with
        other transactions by retrying the entire transaction, meaning that any
        counts obtained within the transaction may not reflect what is actually
        changed by the time the transaction successfully commits.

        We don't update the rowcount property after SELECT or SELECTV, because
        updating .rowcount requires calling `cdb2_get_effects`, which consumes
        any outstanding result set.  This would consume the result set before
        the user could iterate over it.  We also don't update the rowcount
        property after EXEC PROCEDURE, because a stored procedure could emit
        a result set.
        """
        self._check_closed()
        return self._rowcount

    # Optional DB API Extension
    @property
    def connection(self):
        """Returns a reference to the connection this cursor belongs to."""
        self._check_closed()
        return self._conn

    def close(self):
        """Close the cursor now.

        The cursor will be unusable from this point forward; an InterfaceError
        exception will be raised if any operation is attempted with the
        cursor.

        Note that this does not roll back any uncommitted operations executed
        by this cursor - a new cursor created off of this cursor's connection
        will start off in the middle of that uncommitted transaction.
        """
        self._check_closed()
        self._description = None
        self._closed = True

    def callproc(self, procname, parameters):
        """Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each argument
        that the procedure expects.

        If the called procedure emits a result set it is made available
        through the fetch methods, or by iterating over the cursor, as though
        it was returned by a select statement.

        Args:
            procname: The name of the stored procedure to be executed.
            parameters: A sequence of strings to be passed, in order, as the
                arguments to the stored procedure.

        Returns:
            A copy of the input parameters.
        """
        if not _VALID_SP_NAME.match(procname):
            raise NotSupportedError("Invalid procedure name '%s'" % procname)
        params_as_dict = {str(i): e for i, e in enumerate(parameters)}
        sql = ("exec procedure " + procname + "("
              + ", ".join("%%(%d)s" % i for i in range(len(params_as_dict)))
              + ")")
        self.execute(sql, params_as_dict)
        return parameters[:]

    def execute(self, sql, parameters=None):
        """Execute a database operation (query or command).

        Parameters must be provided as a mapping and will be bound to variables
        in the operation.  The sql string must be provided as a Python format
        string, with parameter names represented as `%(name)s` and all other
        `%` signs escaped as `%%`.

        Args:
            sql: The SQL string to execute, as a Python format string.
            parameters: An optional mapping from parameter names to the values
                to be bound for them.

        Returns:
            This cursor, which can be used as an iterator over the result set
            returned by the query.  When iterating over the cursor, one list
            will be yielded per row in the result set, where the elements in
            the list correspond to the result columns within the row, in
            positional order.

        Example:
            >>> c = connection.cursor()
            >>> for row in c.execute("select 1, 2 UNION select %(x)s, %(y)s",
            ...                      {'x': 2, 'y': 4}):
            ...     print row[1], row[0]
            4 2
            2 1
        """
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
        """Execute the same sql statement repeatedly with different parameters.

        This is currently equivalent to calling execute multiple times, once
        for each mapping provided in seq_of_parameters.

        Args:
            sql: The SQL string to execute, as a Python format string.
            parameters: An sequence of mappings from parameter names to the
                values to be bound for them.
        """
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

        try:
            # If variable interpolation fails, then translate the exception to
            # an InterfaceError to signal that it's a client-side problem.
            sql = sql % {name: "@" + name for name in parameters}
        except KeyError as keyerr:
            raise InterfaceError("No value provided for parameter %s" % keyerr)
        except Exception:
            raise InterfaceError("Invalid Python format string for query")

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
        """No-op; implemented for PEP-249 compliance."""
        self._check_closed()

    def setoutputsize(self, size, column=None):
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
        self._description = tuple((name, type, None, None, None, None, None)
                                  for name, type in zip(names, types))
        if not self._description:
            self._description = None

    def fetchone(self):
        """Fetch the next row of the current result set.

        Returns:
            A list, where the elements in the list correspond to the result
            columns within the next row in the result set in positional order.
            If all rows have been consumed, None is returned.
        """
        try:
            return next(self)
        except StopIteration:
            return None

    def fetchmany(self, n=None):
        """Fetch the next set of rows of the current result set.

        Args:
            n: Maximum number of rows to be returned.  If this argument is not
                given, the cursor's arraysize property is used as the maximum.

        Returns:
            A sequence of up to n lists, where the elements in each list
            correspond to the result columns within the next row in the result
            set in positional order.  If fewer than n rows are available, the
            returned sequence will have fewer than n lists.  If no rows are
            available, the returned sequence will have no elements.
        """
        if n is None:
            n = self.arraysize
        return [x for x in itertools.islice(self, 0, n)]

    def fetchall(self):
        """Fetch all remaining rows of the current result set.

        Returns:
            All remaining rows of the current result set, as a list of lists.
        """
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
