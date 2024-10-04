.. _Comdb2 to Python Type Mappings:

******************************
Comdb2 to Python Type Mappings
******************************

For all Comdb2 types, the same Python type is used for binding a parameter
value as is returned for a SQL query result column of that type.  SQL types are
mapped to Python types according to the following table:

============   ================================================================
SQL type       Python type
============   ================================================================
NULL           ``None``
integer        `int`
real           `float`
blob           `bytes`
text           `str`
datetime       `datetime.datetime`
datetimeus     `~.cdb2.DatetimeUs`
carray         `list` or `tuple` of exactly one of the types above (R8 only)
intervalym     not supported
intervalds     not supported
intervaldsus   not supported
decimal        not supported
============   ================================================================

NULL
====

A SQL NULL value is represented as a Python ``None``.

Numeric Types
=============

Integral SQL values are represented as Python ``int`` values.  Floating point
SQL values are represented as Python ``float`` values.  If a Python value is
given that's too large to fit into the database column (for instance, 1000000
into a column of type ``short``), an exception will be raised.

.. _String and Blob Types:

String and Blob Types
=====================

You may occasionally need to convert between byte strings and Unicode strings:

#.  If you have a variable of type `bytes` and you want to pass it to the
    database as a TEXT value, you need to convert it to a `str` using
    `~bytes.decode`.

#.  When a TEXT column is read back from the database, it is decoded as UTF-8.
    If your database has non-UTF-8 text in a ``cstring`` column, you need to
    cast that column from a TEXT to a BLOB in SQL when reading it out, like
    this::

        cursor.execute("select cast(col as blob) from tbl")

    That will result in you receiving a `bytes` object rather than `str` for
    that result column.

    .. note::
        ASCII is a subset of UTF-8, so this is not required for if the column
        contains plain ASCII text.

#.  Likewise, when a `str` object is sent to the database, it is encoded as
    UTF-8.  If you need to store non-UTF-8 text in a ``cstring`` column in your
    database, you need to bind a byte string in Python land, and then cast the
    value from BLOB to TEXT in SQL.  For instance, to bind a latin1 string
    containing a copyright symbol, you could do::

        params = {'c': b'Copyright \xA9 2016'}
        # dbapi2
        c.execute("select * from tbl where col=cast(%(c)s as text)", params)
        # cdb2
        h.execute("select * from tbl where col=cast(@c as text)", params)

Date and Time Types
===================

Comdb2 has two types for representing a specific point in time.  The DATETIME
type supports millisecond precision and is mapped to Python's
`datetime.datetime`. The DATETIMEUS type supports microsecond precision and is
mapped to a subclass of datetime.datetime that we expose, called
`~.cdb2.DatetimeUs`.  This subclass behaves identically to
`datetime.datetime`, differing only in type and in resulting in higher
precision values when bound as an input parameter or retrieved as a result
column.

Whenever either type of datetime is retrieved as a result column, the returned
`datetime.datetime` object will be aware, meaning it will be associated with
a timezone (namely, the current timezone of the Comdb2 connection). Whenever
either type of datetime is sent as a query parameter, it is permitted to be
naïve, in which case it uses the current timezone of the Comdb2 connection.  By
default that timezone is UTC but it can be changed on a per-connection basis if
needed.

Comdb2 additionally supports three types for representing time intervals. One
represents an exact number of elapsed milliseconds between two times
(INTERVALDS). One represents an exact number of elapsed microseconds between
two times (INTERVALDSUS). The final one (INTERVALYM) represents a number of
years and months between two events - unlike the other two, this represents
a fuzzy time interval, as the length of a month varies. We do not currently
support any of these types.

Decimal Type
============

Comdb2 supports a DECIMAL type. However, when retriving these columns through
``libcdb2api``, they are returned as TEXT, making them indistinguishable from
string columns.  That currently makes it impossible to map them to Python's
`decimal.Decimal` type as we would like to.  If ``libcdb2api`` is ever changed
to properly distinguish between DECIMAL and TEXT columns this package will be
enhanced to properly expose DECIMAL columns.

C Array Types
=============

.. note::
   This only works with comdb2 R8. If you need to connect to a database that is
   still running comdb2 R7, the queries using this feature will fail.

Often in SQL queries, it is useful to bind an array of elements of the same
type, for example when using the ``IN`` operator. In this case, you can use a
(non-empty) `list` or `tuple`, as long as all elements of the sequence are of
the same type (and one of the above Python types). Note that nested sequences
are not allowed.

Note that when binding an element of that type, you want to use the ``CARRAY``
function, like so::

   params = {'arr': [1, 2, 3, 4]}
   # dbapi2
   c.execute("select 2 in CARRAY(%(arr)s)", params)
   # cdb2
   h.execute("select 2 in CARRAY(@arr)", params)
