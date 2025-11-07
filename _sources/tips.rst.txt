********************************
Best Practices, Tips, and Tricks
********************************

#. This package uses `str` for text and `bytes` for blobs. See :doc:`types` for
   an explanation of what Python types must be used when binding parameters,
   and what Python types will be returned for result columns.

   An error message saying "incompatible values from SQL blob of length 3 to
   bcstring field 'foo'" most likely means that you passed a byte string where
   a unicode string should have been passed.

   An error message saying "incompatible values from SQL string of length 3 to
   bytearray field 'bar'" most likely means that you passed a unicode string
   where a byte string should have been passed.  Note the byte string must be 
   *exactly* the length of the column.  No padding is performed automatically,
   unless the column is declared with a ``dbpad`` attribute in the csc2 schema 
   (for example ``dbpad=0`` will pad with null bytes or ``dbpad=32`` will pad 
   with spaces).

   Not all places where you pass the wrong type of string will result in an
   exception - in some cases the SQL query will appear to succeed but
   a ``WHERE`` clause that you expected to match some rows will silently fail
   to match any.

#. The latest version of this package only supports Python 3. If you can't
   use Python 3, make sure to use version less than ``1.5.0``.

#. The database can time out connections that have been idle for a period of
   time, and each idle connection uses some amount of resources on the database
   server, so avoid leaving sessions open when they're not in use.  As an
   example, prefer opening a new database connection for each client request in
   a service.

#. Running a SQL statement using the `.dbapi2` interface will implicitly open
   a transaction, as per PEP-249.  This means that, unlike other Comdb2
   interfaces, you need to explicitly call `.Connection.commit` to save your
   changes.  If you write a program that seems to succeed but doesn't make the
   updates you expected, make sure you didn't miss a call to
   `~.Connection.commit`.

#. When transactions aren't needed, prefer using :ref:`Autocommit Mode` on your
   `.dbapi2.Connection` objects.  It results in less bookkeeping on the
   database server, and results in transparent retries after some error
   conditions - and it makes the behavior more consistent with other Comdb2
   interfaces.

#. Always prefer binding parameters over dynamically generating SQL queries.
   Instead of this::

       c.execute("SELECT * FROM test where id=%s" % my_id)

   Always do this::

       c.execute("SELECT * FROM test where id=%(id)s", {'id': my_id})

   Because of this: https://xkcd.com/327/

   When using comdb2 R8, you can even bind a `list` or `tuple` object like so::

       c.execute(
           "SELECT %(needle)s IN CARRAY(%(haystack)s)",
           {'needle': 5, 'haystack': [1, 2, 3, 4, 5]}
       )

   .. note::
       The two modules provided by this package use different syntax for SQL
       placeholders.  See `.dbapi2.Cursor.execute` and `.cdb2.Handle.execute`
       for details.

#. When using `.dbapi2` but not parameter binding by position be sure to escape
   any literal ``%`` signs in a query by doubling them.  That is, instead of::

        cursor.execute(
            "select * from log where msg like 'ERROR: %' and pid = %(pid)s",
            {"pid": 1234},
        )

   You need to write::

        cursor.execute(
            "select * from log where msg like 'ERROR: %%' and pid = %(pid)s",
            {"pid": 1234},
        )

   Also, if you don't want to pass any parameters to a `.dbapi2` query, it's
   better to pass ``()`` than the default ``None`` value, because this removes
   the need to escape any literal ``%`` signs that appear in the query::

        cursor.execute(
            "select * from log where msg like 'ERROR: %'",
            (),
        )

   See `.dbapi2.paramstyle` for an explanation of why.

#. Unlike many other databases, Comdb2 does not allow you to read uncommitted
   rows by default, even from the cursor that created them.  If you need to be
   able to read your own uncommitted work, you must execute ``set transaction
   read committed`` as the first statement on a new `.dbapi2.Connection` or
   `.cdb2.Handle` (any isolation level higher than ``READ COMMITTED`` would
   obviously work as well).

#. When using comdb2 R8, the library supports binding of (non-empty) `list` or
   `tuple` objects with the help of the ``CARRAY`` function, as long as all
   elements of the sequence are of the same type::

        from comdb2.dbapi2 import connect

        def search_in_list(needle, haystack):
            params = {'needle': needle, 'haystack': haystack}
            sql = "select %(needle)s in CARRAY(%(haystack)s)"
            print(connect('mattdb').cursor().execute(sql, params).fetchall())

        haystack = [1, 2, 3, 4, 5]
        search_in_list(0, haystack)
        search_in_list(5, haystack)

  If you still need to use R7, you would want to generate the query
  dynamically, for example::

        from comdb2.dbapi2 import connect

        def _generate_bound_list(prefix, items):
            params = {prefix + str(i): e for i, e in enumerate(items)}
            sql_frag = "(" + ", ".join("%%(%s)s" % p for p in params) + ")"
            return params, sql_frag

        def search_in_list(needle, haystack):
            haystack_params, in_haystack_sql = _generate_bound_list("hs", haystack)
            params = {'needle': needle}
            params.update(haystack_params)

            sql = "select %(needle)s in " + in_haystack_sql
            print(connect('mattdb').cursor().execute(sql, params).fetchall())

        haystack = [1, 2, 3, 4, 5]
        search_in_list(0, haystack)
        search_in_list(5, haystack)

