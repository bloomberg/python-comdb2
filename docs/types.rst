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
blob           `six.binary_type` (aka `bytes` in Python 3, ``str`` in Python 2)
text           `six.text_type` (aka `str` in Python 3, ``unicode`` in Python 2)
datetime       `datetime.datetime`
datetimeus     `~.cdb2.DatetimeUs`
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

This module uses byte strings to represent BLOB values, and Unicode strings to
represent TEXT columns.  This was chosen for maximum forward compatibility with
Python 3, and to make it easier to write code that will work identically in
both languages.  This decision has many important ramifications.

#.  In Python 2, ``'foo'`` is the wrong type for binding a ``cstring`` column.
    You instead need to use a Unicode literal like ``u'foo'``.
    Alternately, you can make all string literals in your module be Unicode
    literals by default by doing::

        from __future__ import unicode_literals

    .. note::
        It's not safe to blindly do this in existing code as it changes that
        code's behavior, and might cause problems.

#.  If you have a variable  of type `six.binary_type` (byte string) and you
    want to pass it to the database as a TEXT value, you need to convert it to
    a `six.text_type` (Unicode) string using `~bytes.decode`.  In Python 2,
    unless `unicode_literals` was imported from `__future__`, you will need to
    do this with every string variable that you want to use as a cstring
    column.  For example::

        value = 'foo'
        params = {'col': value.decode('utf-8')}
        # dbapi2
        cursor.execute("select * from tbl where col=%(col)s", params)
        # cdb2
        handle.execute("select * from tbl where col=@col", params)

#.  TEXT columns are returned to you as `six.text_type` (Unicode) strings.
    If you need to pass them to a library that expects `six.binary_type` (byte)
    strings, you have to use `~str.encode`, like this::

        cursor.execute("select col from tbl")
        for row in cursor:
            col = row[0]
            library_func(col.encode('utf-8'))

    This may come up a lot in Python 2, where historically libraries were
    written to expect byte strings rather than Unicode strings.

#.  When a TEXT column is read back from the database, it is decoded as UTF-8.
    If your database has non-UTF-8 text in a ``cstring`` column, you need to
    cast that column from a TEXT to a BLOB in SQL when reading it out, like
    this::

        cursor.execute("select cast(col as blob) from tbl")

    That will result in you receiving a byte string rather than a Unicode
    string for that result column.

    .. note::
        ASCII is a subset of UTF-8, so this is not required for if the column
        contains plain ASCII text.

#.  Likewise, when a Unicode string is sent to the database, it is encoded as
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
