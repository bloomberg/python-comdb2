# cython: c_string_type=str, c_string_encoding=utf-8

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

"""Thin Python wrapper over libcdb2api"""

from libc.string cimport strcmp, strcpy
from cpython.datetime cimport (import_datetime, PyTypeObject, timedelta_new,
                               PyDateTimeAPI)
from cpython.ref cimport Py_TYPE, PyObject
from cpython.mem cimport PyMem_Malloc, PyMem_Free

import datetime

from pytz import timezone, UTC
import six

from ._cdb2_types import Error, Effects, DatetimeUs
from . cimport _cdb2api as lib

import_datetime()


# XXX Hack to get a PyTypeObject for DatetimeUs
cdef PyTypeObject *DatetimeUsType = Py_TYPE(DatetimeUs(1, 1, 1))


# Template type to reduce code duplication for the two datetime types
ctypedef fused client_datetime:
    lib.cdb2_client_datetime_t
    lib.cdb2_client_datetimeus_t


cdef _string_as_bytes(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    elif type(s) is bytes:
        return s
    raise TypeError("unicode object or byte string required")


cdef _column_type_name(int type):
    if   type == lib.CDB2_REAL:         return "CDB2_REAL"
    elif type == lib.CDB2_INTERVALDSUS: return "CDB2_INTERVALDSUS"
    elif type == lib.CDB2_INTEGER:      return "CDB2_INTEGER"
    elif type == lib.CDB2_CSTRING:      return "CDB2_CSTRING"
    elif type == lib.CDB2_DATETIME:     return "CDB2_DATETIME"
    elif type == lib.CDB2_BLOB:         return "CDB2_BLOB"
    elif type == lib.CDB2_INTERVALYM:   return "CDB2_INTERVALYM"
    elif type == lib.CDB2_DATETIMEUS:   return "CDB2_DATETIMEUS"
    elif type == lib.CDB2_INTERVALDS:   return "CDB2_INTERVALDS"
    else: return "type %d" % type


cdef _errchk(int rc, lib.cdb2_hndl_tp *hndl):
    if rc != lib.CDB2_OK:
        # XXX Coerce to bytes instead of str to gracefully handle non-UTF-8
        #     error messages - just in case.
        raise Error(rc, <bytes>lib.cdb2_errstr(hndl))


cdef _closed_connection_error(method):
    return Error(lib.CDB2ERR_NOTCONNECTED,
                 method + "() called on a closed connection")


cdef _describe_exception(exc):
    return "%s: %s" % (type(exc).__name__, str(exc))


cdef _bind_datetime(obj, client_datetime *val):
    if client_datetime is lib.cdb2_client_datetimeus_t:
        val.usec = obj.microsecond
    else:
        # Round to the nearest millisecond (this was an arbitrary decision
        # but is now needed for backwards compatibility).
        obj += timedelta_new(0, 0, 500)
        val.msec = obj.microsecond // 1000

    struct_time = obj.utctimetuple()
    val.tm.tm_sec = struct_time.tm_sec
    val.tm.tm_min = struct_time.tm_min
    val.tm.tm_hour = struct_time.tm_hour
    val.tm.tm_mday = struct_time.tm_mday
    val.tm.tm_mon = struct_time.tm_mon - 1
    val.tm.tm_year = struct_time.tm_year - 1900
    val.tm.tm_wday = struct_time.tm_wday
    val.tm.tm_yday = struct_time.tm_yday - 1
    val.tm.tm_isdst = struct_time.tm_isdst
    strcpy(val.tzname, b"UTC")


cdef class _ParameterValue(object):
    cdef int type
    cdef int size
    cdef void *data
    cdef object owner

    def __cinit__(self, obj):
        try:
            if obj is None:
                self.type = lib.CDB2_INTEGER
                self.owner = None
                self.size = 0
                self.data = NULL
                return
            elif isinstance(obj, six.integer_types):
                self.type = lib.CDB2_INTEGER
                self.owner = None
                self.size = sizeof(long long)
                self.data = PyMem_Malloc(self.size)
                (<long long*>self.data)[0] = obj
                return
            elif isinstance(obj, float):
                self.type = lib.CDB2_REAL
                self.owner = None
                self.size = sizeof(double)
                self.data = PyMem_Malloc(self.size)
                (<double*>self.data)[0] = obj
                return
            elif isinstance(obj, bytes):
                self.type = lib.CDB2_BLOB
                self.owner = obj
                self.size = len(self.owner)
                self.data = <char*>self.owner
                return
            elif isinstance(obj, unicode):
                self.type = lib.CDB2_CSTRING
                self.owner = obj.encode('utf-8')
                self.size = len(self.owner)
                self.data = <char*>self.owner
                return
            elif isinstance(obj, DatetimeUs):
                self.type = lib.CDB2_DATETIMEUS
                self.owner = None
                self.size = sizeof(lib.cdb2_client_datetimeus_t)
                self.data = PyMem_Malloc(self.size)
                _bind_datetime(obj, <lib.cdb2_client_datetimeus_t*>self.data)
                return
            elif isinstance(obj, datetime.datetime):
                self.type = lib.CDB2_DATETIME
                self.owner = None
                self.size = sizeof(lib.cdb2_client_datetime_t)
                self.data = PyMem_Malloc(self.size)
                _bind_datetime(obj, <lib.cdb2_client_datetime_t*>self.data)
                return
        except Exception as e:
            exc = e
        else:
            exc = None

        exc_desc = _describe_exception(exc)

        if exc is not None:
            errmsg = "Can't bind value %r: %s" % (obj, exc_desc)
            six.raise_from(Error(lib.CDB2ERR_CONV_FAIL, errmsg), exc)
        else:
            errmsg = "No Comdb2 type mapping for parameter %r" % obj
            raise Error(lib.CDB2ERR_NOTSUPPORTED, errmsg)


    def __dealloc__(self):
        if self.owner is None:
            PyMem_Free(self.data)


cdef _make_datetime(client_datetime *val):
    if client_datetime is lib.cdb2_client_datetime_t:
        pytype = PyDateTimeAPI.DateTimeType
        usec = val.msec * 1000
    else:
        pytype = DatetimeUsType
        usec = val.usec

    if 0 == strcmp(val.tzname, b"UTC"):
        return PyDateTimeAPI.DateTime_FromDateAndTime(
                    val.tm.tm_year + 1900,
                    val.tm.tm_mon + 1,
                    val.tm.tm_mday,
                    val.tm.tm_hour,
                    val.tm.tm_min,
                    val.tm.tm_sec,
                    usec,
                    UTC,
                    pytype)

    return timezone(val.tzname).localize(
        PyDateTimeAPI.DateTime_FromDateAndTime(
                    val.tm.tm_year + 1900,
                    val.tm.tm_mon + 1,
                    val.tm.tm_mday,
                    val.tm.tm_hour,
                    val.tm.tm_min,
                    val.tm.tm_sec,
                    usec,
                    None,
                    pytype),
        val.tm.tm_isdst)


cdef _column_value(lib.cdb2_hndl_tp *hndl, int col):
    column_value = lib.cdb2_column_value(hndl, col)
    if column_value is NULL:
        return None

    cdef int size
    try:
        column_type = lib.cdb2_column_type(hndl, col)
        if column_type == lib.CDB2_INTEGER:
            return (<long long*>column_value)[0]
        elif column_type == lib.CDB2_REAL:
            return (<double*>column_value)[0]
        elif column_type == lib.CDB2_BLOB:
            size = lib.cdb2_column_size(hndl, col)
            return <bytes>((<char*>column_value)[:size])
        elif column_type == lib.CDB2_CSTRING:
            size = lib.cdb2_column_size(hndl, col)
            return <unicode>((<char*>column_value)[:size - 1])
        elif column_type == lib.CDB2_DATETIMEUS:
            return _make_datetime(<lib.cdb2_client_datetimeus_t*>column_value)
        elif column_type == lib.CDB2_DATETIME:
            return _make_datetime(<lib.cdb2_client_datetime_t*>column_value)
    except Exception as e:
        exc = e
    else:
        exc = None

    errmsg = "Failed to decode %s column %d ('%s'): " % (
        _column_type_name(column_type), col, lib.cdb2_column_name(hndl, col))

    if exc is not None:
        errmsg += _describe_exception(exc)
        six.raise_from(Error(lib.CDB2ERR_CONV_FAIL, errmsg), exc)
    else:
        errmsg += "Unsupported column type"
        raise Error(lib.CDB2ERR_NOTSUPPORTED, errmsg)


cdef class _Cursor(object):
    """Iterator over Comdb2 results sets"""

    cdef Handle handle
    cdef object row_class

    def __init__(self, *args, **kwargs):
        raise TypeError("This class cannot be instantiated from Python")

    def __iter__(self):
        return self

    def __next__(self):
        if <PyObject*>self != self.handle.cursor:
            raise Error(lib.CDB2ERR_NOTSUPPORTED,
                        "cursor used after invalidation")

        cdef lib.cdb2_hndl_tp *hndl = self.handle.hndl
        if not hndl: raise _closed_connection_error('cursor next')

        with nogil:
            rc = lib.cdb2_next_record(hndl)

        if rc == lib.CDB2_OK_DONE:
            raise StopIteration()

        _errchk(rc, hndl)

        num_columns = lib.cdb2_numcolumns(hndl)
        ret = [_column_value(hndl, i) for i in range(num_columns)]
        if self.row_class is not None:
            try:
                ret = self.row_class(ret)
            except Exception as e:
                errmsg = "Instantiating row failed: " + _describe_exception(e)
                six.raise_from(Error(lib.CDB2ERR_UNKNOWN, errmsg), e)
        return ret


cdef class Handle(object):
    """Handle(database, tier[, flags]) -> handle to a Comdb2 database"""

    cdef lib.cdb2_hndl_tp *hndl
    cdef PyObject *cursor
    cdef object _row_factory

    def __init__(self, database_name, tier=b'default', flags=0):
        database_name = _string_as_bytes(database_name)
        tier = _string_as_bytes(tier)
        cdef char *c_database_name = database_name
        cdef char *c_tier = tier
        cdef int c_flags = flags
        with nogil:
            rc = lib.cdb2_open(&self.hndl, c_database_name, c_tier, c_flags)
        _errchk(rc, self.hndl)

    def __dealloc__(self):
        if self.hndl:
            with nogil:
                lib.cdb2_close(self.hndl)

    def close(self):
        """close(): close this handle's Comdb2 connection"""
        if not self.hndl: raise _closed_connection_error('close')
        with nogil:
            rc = lib.cdb2_close(self.hndl)
        self.hndl = NULL
        _errchk(rc, self.hndl)

    def execute(self, sql, parameters=None):
        """execute(sql, params) -> Cursor over a SQL statement's result set"""
        if not self.hndl: raise _closed_connection_error('execute')
        self.cursor = NULL
        sql = _string_as_bytes(sql)
        cdef char *c_sql = sql

        param_guards = []
        try:
            if parameters is not None:
                for key, val in parameters.items():
                    ckey = _string_as_bytes(key)
                    cval = _ParameterValue(val)
                    param_guards.append(ckey)
                    param_guards.append(cval)
                    rc = lib.cdb2_bind_param(self.hndl, <char*>ckey,
                                             cval.type, cval.data, cval.size)
                    _errchk(rc, self.hndl)

            with nogil:
                while lib.cdb2_next_record(self.hndl) == lib.CDB2_OK:
                    pass  # consume any previous result set
                rc = lib.cdb2_run_statement(self.hndl, c_sql)
            _errchk(rc, self.hndl)
        finally:
            rc = lib.cdb2_clearbindings(self.hndl)
        _errchk(rc, self.hndl)

        return self

    def column_names(self):
        """column_names() -> list of result set column names"""
        if not self.hndl: raise _closed_connection_error('column_names')
        return [<unicode>lib.cdb2_column_name(self.hndl, i)
                for i in range(lib.cdb2_numcolumns(self.hndl))]

    def column_types(self):
        """column_types() -> list of result set column types"""
        if not self.hndl: raise _closed_connection_error('column_types')
        return [lib.cdb2_column_type(self.hndl, i)
                for i in range(lib.cdb2_numcolumns(self.hndl))]

    def get_effects(self):
        """get_effects() -> Effects of the current or last transaction"""
        if not self.hndl: raise _closed_connection_error('get_effects')
        self.cursor = NULL
        cdef lib.cdb2_effects_tp effects
        with nogil:
            rc = lib.cdb2_get_effects(self.hndl, &effects)
        _errchk(rc, self.hndl)
        return Effects(effects.num_affected,
                       effects.num_selected,
                       effects.num_updated,
                       effects.num_deleted,
                       effects.num_inserted)

    def __iter__(self):
        if self._row_factory is None:
            row_class = None
        else:
            try:
                row_class = self._row_factory(self.column_names())
            except Exception as e:
                errmsg = "row_factory call failed: " + _describe_exception(e)
                six.raise_from(Error(lib.CDB2ERR_UNKNOWN, errmsg), e)

        cdef _Cursor ret = _Cursor.__new__(_Cursor)
        ret.handle = self
        ret.row_class = row_class
        self.cursor = <PyObject*>ret
        return ret

    property row_factory:
        """Factory for creating the result row type"""

        def __get__(self):
            return self._row_factory

        def __set__(self, value):
            if not callable(value):
                raise TypeError("row_factory must be a callable")
            self._row_factory = value
