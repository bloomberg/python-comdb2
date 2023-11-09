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

cdef extern from "cdb2api.h" nogil:
    enum:
        CDB2_OK
        CDB2_OK_DONE
        CDB2ERR_UNKNOWN
        CDB2ERR_NOTCONNECTED
        CDB2ERR_NOTSUPPORTED
        CDB2ERR_CONV_FAIL

    enum cdb2_coltype:
        CDB2_INTEGER
        CDB2_REAL
        CDB2_CSTRING
        CDB2_BLOB
        CDB2_DATETIME
        CDB2_INTERVALYM
        CDB2_INTERVALDS
        CDB2_DATETIMEUS
        CDB2_INTERVALDSUS

    enum:
        CDB2_MAX_TZNAME

    ctypedef struct cdb2_tm_t:
        int tm_sec
        int tm_min
        int tm_hour
        int tm_mday
        int tm_mon
        int tm_year
        int tm_wday
        int tm_yday
        int tm_isdst

    ctypedef struct cdb2_effects_tp:
        int num_affected
        int num_selected
        int num_updated
        int num_deleted
        int num_inserted

    ctypedef struct cdb2_client_datetime_t:
        cdb2_tm_t           tm
        unsigned int        msec
        char                tzname[CDB2_MAX_TZNAME]

    ctypedef struct cdb2_client_datetimeus_t:
        cdb2_tm_t           tm
        unsigned int        usec
        char                tzname[CDB2_MAX_TZNAME]

    ctypedef struct cdb2_hndl_tp:
        pass

    int cdb2_open(cdb2_hndl_tp **hndl, const char *dbname, const char *type, int flags);
    int cdb2_next_record(cdb2_hndl_tp *hndl);
    int cdb2_get_effects(cdb2_hndl_tp *hndl, cdb2_effects_tp *effects);
    int cdb2_close(cdb2_hndl_tp* hndl);
    int cdb2_run_statement(cdb2_hndl_tp *hndl, const char *sql);
    int cdb2_numcolumns(cdb2_hndl_tp* hndl);
    const char* cdb2_column_name(cdb2_hndl_tp* hndl, int col);
    int cdb2_column_type(cdb2_hndl_tp* hndl, int col);
    int cdb2_column_size(cdb2_hndl_tp* hndl, int col);
    void* cdb2_column_value(cdb2_hndl_tp* hndl, int col);
    const char* cdb2_errstr(cdb2_hndl_tp* hndl);
    int cdb2_bind_param(cdb2_hndl_tp *hndl, const char *name, int type, const void *varaddr, int length);
    int cdb2_bind_array(cdb2_hndl_tp *hndl, const char *name, cdb2_coltype, const void *varaddr, size_t count, size_t typelen);
    int cdb2_clearbindings(cdb2_hndl_tp *hndl);
    int cdb2_clear_ack(cdb2_hndl_tp *hndl);
