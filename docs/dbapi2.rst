***********************************************
`comdb2.dbapi2` DBAPI-2.0 compatible Comdb2 API
***********************************************

.. automodule:: comdb2.dbapi2
    :synopsis: DBAPI-2.0 compatible Comdb2 API

API Documentation
=================

Constants
---------

The DB-API requires several constants that describe the capabilities and
behavior of the module.

.. autodata:: apilevel

.. autodata:: threadsafety

.. autodata:: paramstyle

Connections and Cursors
-----------------------

The user interacts with the database through `Connection` and `Cursor` objects.

.. autofunction:: connect

.. autoclass:: Connection
    :members:
    :exclude-members: Error, Warning, InterfaceError, DatabaseError,
        InternalError, OperationalError, ProgrammingError, IntegrityError,
        DataError, NotSupportedError

.. autoclass:: Cursor()
    :members:

    .. automethod:: __iter__

Type Objects
------------

Several constants are provided that are meant to be compared for equality
against the type codes returned by `Cursor.description`.

.. autodata:: STRING

.. autodata:: BINARY

.. autodata:: NUMBER

.. autodata:: DATETIME

.. autodata:: ROWID
    :annotation: = comdb2.dbapi2.STRING

    This is required by PEP-249, but we just make it an alias for
    `STRING`, because Comdb2 doesn't have a ROWID result type.

Type Constructors
-----------------

The DB-API requires constructor functions for DATETIME and BLOB parameter
values, since different databases might want to use different types to
represent them.  We use `datetime.datetime` objects for DATETIME columns, and
`bytes` objects for BLOB columns - you may use these constructors to create
them if you so choose, but you are not required to.

.. function:: Timestamp(year, month, day, hour, minute, second)

    Creates an object suitable for binding as a DATETIME parameter.

    :returns: An object representing the given date and time
    :rtype: `datetime.datetime`

.. function:: TimestampFromTicks(seconds_since_epoch)

    Creates an object suitable for binding as a DATETIME parameter.

    :param float seconds_since_epoch: An offset from the Unix epoch, in seconds
    :returns: An object representing the date and time ``seconds_since_epoch``
        after the Unix epoch, with millisecond precision
    :rtype: `datetime.datetime`

.. function:: Binary(string)

    Creates an object suitable for binding as a BLOB parameter.

    If the input argument was a `six.text_type` Unicode string, it is
    encoded as a UTF-8 byte string and returned.  Otherwise, the input
    argument is passed to the `bytes` constructor, and the result returned.

    :param string: A string from which the new object is constructed
    :rtype: `bytes`
    :returns: A byte string representing the given input

DatetimeUs
----------

A class is provided for differentiating Comdb2's DATETIMEUS type from its
DATETIME type.

 .. autoclass:: DatetimeUs(year, month, day[, hour[, minute[, second[, microsecond[, tzinfo]]]]])

    .. automethod:: fromdatetime(datetime)

Additionally, two constructor functions are provided for DATETIMEUS parameters,
for consistency with the required DATETIME constructors documented above.

.. function:: TimestampUs(year, month, day, hour, minute, second)

    Creates an object suitable for binding as a DATETIMEUS parameter.

    :returns: An object representing the given date and time
    :rtype: `DatetimeUs`

.. function:: TimestampUsFromTicks(seconds_since_epoch)

    Creates an object suitable for binding as a DATETIMEUS parameter.

    :param float seconds_since_epoch: An offset from the Unix epoch, in seconds
    :returns: An object representing the date and time ``seconds_since_epoch``
        after the Unix epoch, with microsecond precision
    :rtype: `DatetimeUs`

.. _Exceptions:

Exceptions
----------

.. autoexception:: Error

.. autoexception:: Warning

.. autoexception:: InterfaceError

.. autoexception:: DatabaseError

.. autoexception:: InternalError

.. autoexception:: OperationalError

.. autoexception:: ProgrammingError

.. autoexception:: IntegrityError

.. autoexception:: UniqueKeyConstraintError

.. autoexception:: ForeignKeyConstraintError

.. autoexception:: NonNullConstraintError

.. autoexception:: DataError

.. autoexception:: NotSupportedError

This is the exception inheritance layout::

    Exception
     +-- Warning
     +-- Error
          +-- InterfaceError
          +-- DatabaseError
               +-- DataError
               +-- OperationalError
               +-- IntegrityError
               |    +-- UniqueKeyConstraintError
               |    +-- ForeignKeyConstraintError
               |    +-- NonNullConstraintError
               +-- InternalError
               +-- ProgrammingError
               +-- NotSupportedError

.. rubric:: Exceptions for Polymorphic Clients

Most exceptions types that can be raised by this module are also exposed as
attributes on the `Connection` class:

.. attribute::
    comdb2.dbapi2.Connection.Error
    comdb2.dbapi2.Connection.Warning
    comdb2.dbapi2.Connection.InterfaceError
    comdb2.dbapi2.Connection.DatabaseError
    comdb2.dbapi2.Connection.InternalError
    comdb2.dbapi2.Connection.OperationalError
    comdb2.dbapi2.Connection.ProgrammingError
    comdb2.dbapi2.Connection.IntegrityError
    comdb2.dbapi2.Connection.DataError
    comdb2.dbapi2.Connection.NotSupportedError

    Aliases for `Error` and its subclasses.  This is an optional extension to
    the DB-API specification, designed to simplify writing polymorphic code
    that works with any type of DB-API connection.

    .. note::
        In order to make any meaningful use of this feature, you need to be
        writing code that could be passed a connection created by one of at
        least two different DB-API compliant modules, and both of those modules
        must implement this optional extension.
