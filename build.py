from cffi import FFI

ffi = FFI()

ffi.set_source("_cdb2api",
    """
        #include <cdb2api.h>
    """,
    libraries=["cdb2api", "protobuf-c"])
ffi.cdef("""
    enum cdb2_hndl_alloc_flags {
        CDB2_READ_INTRANS_RESULTS,
        CDB2_DIRECT_CPU,
        CDB2_RANDOM,
        CDB2_RANDOMROOM,
        ...
    };

    enum cdb2_errors {
        CDB2_OK,
        CDB2_OK_DONE,
        CDB2ERR_CONNECT_ERROR,
        CDB2ERR_NOTCONNECTED,
        CDB2ERR_PREPARE_ERROR,
        CDB2ERR_IO_ERROR,
        CDB2ERR_INTERNAL,
        CDB2ERR_NOSTATEMENT,
        CDB2ERR_BADCOLUMN,
        CDB2ERR_BADSTATE,
        CDB2ERR_ASYNCERR,
        CDB2_OK_ASYNC,

        CDB2ERR_INVALID_ID,
        CDB2ERR_RECORD_OUT_OF_RANGE,

        CDB2ERR_REJECTED,
        CDB2ERR_STOPPED,
        CDB2ERR_BADREQ,
        CDB2ERR_DBCREATE_FAILED,

        CDB2ERR_THREADPOOL_INTERNAL,  /* some error in threadpool code */
        CDB2ERR_READONLY,

        CDB2ERR_NOMASTER,
        CDB2ERR_UNTAGGED_DATABASE,
        CDB2ERR_CONSTRAINTS,
        CDB2ERR_DEADLOCK,

        CDB2ERR_TRAN_IO_ERROR,
        CDB2ERR_ACCESS,

        CDB2ERR_TRAN_MODE_UNSUPPORTED,

        CDB2ERR_VERIFY_ERROR,
        CDB2ERR_FKEY_VIOLATION,
        CDB2ERR_NULL_CONSTRAINT,

        CDB2ERR_CONV_FAIL,
        CDB2ERR_NONKLESS,
        CDB2ERR_MALLOC,
        CDB2ERR_NOTSUPPORTED,

        CDB2ERR_DUPLICATE,
        CDB2ERR_TZNAME_FAIL,

        CDB2ERR_UNKNOWN,
        ...
    };

    /* New comdb2tm definition. */
    typedef struct cdb2_tm
    {
        int tm_sec;
        int tm_min;
        int tm_hour;
        int tm_mday;
        int tm_mon;
        int tm_year;
        int tm_wday;
        int tm_yday;
        int tm_isdst;
    }
    cdb2_tm_t;

    struct cdb2_effects_type {
        int num_affected;
        int num_selected;
        int num_updated;
        int num_deleted;
        int num_inserted;
    };

    /* datetime type definition */
    typedef struct cdb2_client_datetime {
        cdb2_tm_t           tm;
        unsigned int        msec;
        char                tzname[...];
    } cdb2_client_datetime_t;

    /* microsecond-precision datetime type definition */
    typedef struct cdb2_client_datetimeus {
        cdb2_tm_t           tm;
        unsigned int        usec;
        char                tzname[...];
    } cdb2_client_datetimeus_t;

    /* interval types definition */
    typedef struct cdb2_client_intv_ym {
        int                 sign;       /* sign of the interval, +/-1 */
        unsigned int        years;      /* interval year */
        unsigned int        months;     /* interval months [0-11] */
    } cdb2_client_intv_ym_t;

    typedef struct cdb2_client_intv_ds {
        int                 sign;       /* sign of the interval, +/-1 */
        unsigned int        days;       /* interval days    */
        unsigned int        hours;      /* interval hours   */
        unsigned int        mins;       /* interval minutes */
        unsigned int        sec;        /* interval sec     */
        unsigned int        msec;       /* msec             */
    } cdb2_client_intv_ds_t;

    typedef struct cdb2_client_intv_dsus {
        int                 sign;       /* sign of the interval, +/-1 */
        unsigned int        days;       /* interval days    */
        unsigned int        hours;      /* interval hours   */
        unsigned int        mins;       /* interval minutes */
        unsigned int        sec;        /* interval sec     */
        unsigned int        usec;       /* usec             */
    } cdb2_client_intv_dsus_t;

    typedef enum cdb2_coltype {
        CDB2_INTEGER,
        CDB2_REAL,
        CDB2_CSTRING,
        CDB2_BLOB,
        CDB2_DATETIME,
        CDB2_INTERVALYM,
        CDB2_INTERVALDS,
        CDB2_DATETIMEUS,
        CDB2_INTERVALDSUS,
        ...
    } cdb2_coltype;

    typedef struct cdb2_hndl cdb2_hndl_tp;
    typedef struct cdb2_effects_type cdb2_effects_tp;

    void cdb2_set_comdb2db_config(char *cfg_file);
    void cdb2_set_comdb2db_info(char *cfg_info);

    int cdb2_open(cdb2_hndl_tp **hndl, const char *dbname, const char *type, int flags);
    int cdb2_clone(cdb2_hndl_tp **hndl, cdb2_hndl_tp *c_hndl);

    int cdb2_next_record(cdb2_hndl_tp *hndl);

    int cdb2_get_effects(cdb2_hndl_tp *hndl, cdb2_effects_tp *effects);

    int cdb2_close(cdb2_hndl_tp* hndl);

    int cdb2_run_statement(cdb2_hndl_tp *hndl, const char *sql);
    int cdb2_run_statement_typed(cdb2_hndl_tp *hndl, const char *sql, int ntypes, int *types);

    int cdb2_numcolumns(cdb2_hndl_tp* hndl);
    const char* cdb2_column_name(cdb2_hndl_tp* hndl, int col);
    int cdb2_column_type(cdb2_hndl_tp* hndl, int col);
    int cdb2_column_size(cdb2_hndl_tp* hndl, int col);
    void* cdb2_column_value(cdb2_hndl_tp* hndl, int col);
    const char* cdb2_errstr(cdb2_hndl_tp* hndl);

    void cdb2_use_hint(cdb2_hndl_tp* hndl);

    int cdb2_bind_param(cdb2_hndl_tp *hndl, const char *name, int type, const void *varaddr, int length);
    int cdb2_bind_index(cdb2_hndl_tp *hndl, int index, int type, const void *varaddr, int length);
    int cdb2_clearbindings(cdb2_hndl_tp *hndl);

    /* SOCKPOOL CLIENT APIS */
    int cdb2_socket_pool_get(const char *typestr, int dbnum, int *port); /* returns the fd.*/
    void cdb2_socket_pool_donate_ext(const char* typestr, int fd, int ttl, int dbnum, int flags, void *destructor, void *voidargs);

    const char* cdb2_dbname(cdb2_hndl_tp* hndl);
""")

if __name__ == '__main__':
    ffi.compile()
