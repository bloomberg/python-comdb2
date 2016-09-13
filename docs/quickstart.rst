Comdb2 DBAPI quick start
========================

This is a guide for the most common usecases when
using comdb2.dbapi2 for more specific scenarios
check the API documentation.

Creating the connection
=======================

.. code-block:: python

   >>> import comdb2.dbapi2
   >>> conn = comdb2.dbapi2.connect('sample_db')

Retrieving data
===============

Once you have a connection object you can create a cursor
to issue SQL statements to the database.


.. code-block:: python

   >>> c = conn.cursor()

   # Query a table named test, results are in the cursor
   >>> c.execute("SELECT * FROM test LIMIT 1")

   # You can fetch a single value using fetchone
   >>> c.fetchone()
   [5, None, u'hello', None, None, u'world']
   >>> assert c.fetchone() is None # Returns none once exhausted

   # A specified number of rows
   >>> c.execute("SELECT * FROM test")
   >>> c.fetchmany(5) # returns 5 rows
   >>> c.fetchmany(2) # returns 2 rows

   # or get all values with fetchall
   >>> c.execute("SELECT * FROM test")
   >>> c.fetchall()

   # You can also iterate over the cursor to get the results
   >>> c.execute("SELECT * FROM test")
   >>> for row in c:
   >>>     print(row)


Inserting rows
==============

When adding rows into a table, remember that everything
will be run within a transaction, if you want your values
to persist you need to call commit on the connection.

Heads up! The changes are not visible even in your own
connection if you don't commit.

You can read more about Comdb2 isolation levels in
`the Comdb2 documentation page <https://bbgithub.dev.bloomberg.com/pages/comdb2/txnlevels.html>`_

.. code-block:: python

  # Insert multiple values into the test table
  >>> c.execute("INSERT INTO test('int_field', 'string_field', 'utf_field')"
                "VALUES(5, 'hello', 'world')")

  # Save (commit) the changes.
  >>> conn.commit()

  # You can get the number of affected records (after commiting)
  >>> c.rowcount
  1

  # We can also close the connection if we are done with it.
  # Just be sure any changes have been committed or they will be lost.
  >>> conn.close()


Binding values
==============

Thought about doing this?

.. code-block:: python

   >>> c.execute("SELECT * FROM test where id=%s" % my_id)

Please have a look to https://xkcd.com/327/

The proper way to pass variables to the SQL strings is to
bind them.

.. code-block:: python

   >>> c.execute("SELECT * FROM test WHERE int_field=%(in_var)s", dict(in_var=5))

Note that in_var is replaced by the value passed in the
dictionary that matches that key.


Heads up!
=========

No reads until you commit
-------------------------

Your connection won't see your own changes until your commit them.

.. code-block:: python

   >>> import comdb2.dbapi2
   >>> conn = comdb2.dbapi2.connect('bngdb')
   >>> c = conn.cursor()
   >>> len(c.execute("SELECT * FROM test").fetchall())
   12
   >>> c.execute("INSERT INTO test('int_field') values(1)")
   >>> len(c.execute("SELECT * FROM test").fetchall())
   12  # Note you still get 12, you cannot see your own insert till the commit
   >>> conn.commit()
   >>> c.rowcount
   1
   >>> len(c.execute("SELECT * FROM test").fetchall())
   13

This is the default behaviour, you can enable reading your own changes by
running this command as the **first** instruction when creation the cursor:

.. code-block:: python

   >>> c.execute('set transaction read committed')

You get the same cursor back on execute
---------------------------------------

The execute function returns the same cursor it is called with

.. code-block:: python

   >>> c2 = c.execute(<sql>)
   >>> c2 is c  # Same object

bytestrings for blobs, unicode for strings
------------------------------------------

bytestrings (default in python2) are used for blobs and bytes, if you bind
in python2 using a bytestring it won't match your unicode strings in db.

.. code-block:: python

   >>> len(c.execute("SELECT * FROM test where string_field='hello_string'").fetchall())
   1
   >>> len(c.execute("SELECT * FROM test where string_field=%(var)s", dict(var='hello_string')).fetchall())
   0  # No result
   >>> len(c.execute("SELECT * FROM test where string_field=%(var)s", dict(var=u'hello_string')).fetchall())
   1
