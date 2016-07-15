from __future__ import print_function

import itertools
import exceptions

from _cdb2api import ffi, lib

apilevel = "2.0"
threadsafety = 1
paramstyle = "comdb2"

CONVERTER = {
    lib.CDB2_INTEGER: lambda x: ffi.cast("int *", x)[0],
    lib.CDB2_REAL: lambda x: ffi.cast("double *", x)[0],
    lib.CDB2_CSTRING: lambda x: ffi.string(ffi.cast("char*", x))
    # CDB2_BLOB: 
    # CDB2_DATETIME: 
    # CDB2_INTERVALYM: 
    # CDB2_INTERVALDS: 
}

class Error(exceptions.StandardError):
    pass

class Warning(exceptions.StandardError):
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

def _check_rc(rc):
    if rc != lib.CDB2_OK:
        raise Error("FAIL!")

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


class Connection(object):
    def __init__(self, database_name, tier="default", autocommit=False):
        if not autocommit:
            raise NotSupportedError("Only autocommit mode is supported")

        self._connection = ffi.new("struct cdb2_hndl **")
        rc = lib.cdb2_open(self._connection, database_name, tier, 0)
        if rc != lib.CDB2_OK:
            errmsg = ffi.string(lib.cdb2_errstr(self._connection[0]))
            raise OperationalError(errmsg)

    def close(self):
        _check_rc(lib.cdb2_close(self._connection[0]))

    def commit(self):
        cursor = self.cursor()
        cursor.execute("COMMIT")

    def rollback(self):
        cursor = self.cursor()
        cursor.execute("ROLLBACK")

    def cursor(self):
        return Cursor(self._connection)


class Cursor(object):

    def __init__(self, _connection):
        self.arraysize = 1

        self._connection = _connection
        self._description = None
        self._rowcount = -1

        self._closed = False

        self._valid = False

    @property
    def description(self):
        return self._description

    @property
    def rowcount(self):
        return self._rowcount

    def close(self):
        self._closed = True

    def execute(self, sql):
        _check_rc(lib.cdb2_run_statement(self._connection[0], sql))
        self._next_record()
        self._load_description()

    def _next_record(self):
        rc = lib.cdb2_next_record(self._connection[0])

        if rc == lib.CDB2_OK:
            self._valid = True
        elif rc == lib.CDB2_OK_DONE:
            self._valid = False
        else:
            raise Error("Fail!")

    def _num_columns(self):
        return lib.cdb2_numcolumns(self._connection[0])

    def _column_name(self, i):
        return lib.cdb2_column_name(self._connection[0], i)

    def _column_type(self, i):
        return lib.cdb2_column_type(self._connection[0], i)
    
    def _load_description(self):
        self._description = []

        for i in range(self._num_columns()):
            name = ffi.string(self._column_name(i))
            type_ = self._column_type(i)

            self._description.append((name, type_, None, None, None, None, None))

    def fetchone(self):
        try:
            return self.next()
        except StopIteration:
            return None

    def fetchmany(self, n=None):
        if n is None: n = self.arraysize
        return [x for x in itertools.islice(self, 0, n)]

    def fetchall(self):
        return [x for x in self]

    def _load_row(self):
        return tuple(self._load_column(i, d) for i, d in enumerate(self._description))

    def _load_column(self, i, description):
        raw_value = lib.cdb2_column_value(self._connection[0], i)
        if raw_value == ffi.NULL:
            return None
        else:
            return CONVERTER[description[1]](raw_value)

    def __iter__(self):
        return self

    def next(self):
        if not self._valid: raise StopIteration()

        data = self._load_row()
        self._next_record()

        return data

