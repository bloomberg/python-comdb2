Good Practice, Hints & Tips
===========================

1. Comdb2 can timeout connections that aren't used for a period of time. Therefore, open a new
   database context for each client request in a service.
2. Running an SQL statement using the :mod:`comdb2.dbapi2` interface will implicitly open a
   transaction, as per PEP-249. Don't forget to call commit on the connection to save your changes.\
3. :mod:`comdb2` uses `six.text_type` for text and `bytes` for blobs; this may be suprising if
   you're using `str` in Python 2.x.
