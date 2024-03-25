*************************************************
`comdb2.cdb2` Thin, pythonic wrapper over cdb2api
*************************************************

.. automodule:: comdb2.cdb2
    :synopsis: Thin, pythonic wrapper over cdb2api

API Documentation
=================

Handles
-------

The user interacts with the database using `Handle` objects.

.. autoclass:: Handle
    :members:

    .. automethod:: __iter__

Exceptions
----------

.. autoexception:: Error
    :members:

DatetimeUs
----------

A class is provided for differentiating Comdb2's DATETIMEUS type from its
DATETIME type.

 .. autoclass:: DatetimeUs(year, month, day[, hour[, minute[, second[, microsecond[, tzinfo]]]]])

    .. automethod:: fromdatetime(datetime)

Effects
-------

.. autoclass:: Effects
    :members:

Enumerations
------------

Several mappings are provided to expose enumerations from ``cdb2api.h`` to
Python.

 .. autodata:: TYPE

 .. autodata:: HANDLE_FLAGS

 .. autodata:: ERROR_CODE
