from __future__ import unicode_literals, absolute_import

from comdb2 import cdb2
from comdb2.factories import dict_row_factory
from comdb2.factories import namedtuple_row_factory
import pytest
import pytz

COLUMN_LIST = ("short_col u_short_col int_col u_int_col longlong_col"
               " float_col double_col byte_col byte_array_col"
               " cstring_col pstring_col blob_col datetime_col vutf8_col"
               ).split()

#COLUMN_TYPE = (NUMBER, NUMBER, NUMBER, NUMBER, NUMBER,
#               NUMBER, NUMBER, BINARY, BINARY,
#               STRING, STRING, BINARY, DATETIME, STRING)


@pytest.fixture(autouse=True)
def delete_all_rows():
    hndl = cdb2.Handle('mattdb', 'dev')
    while True:
        hndl.execute("delete from all_datatypes limit 100")
        if hndl.get_effects()[0] != 100:
            break
    while True:
        hndl.execute("delete from simple limit 100")
        if hndl.get_effects()[0] != 100:
            break
    hndl.close()


def test_garbage_collecting_unused_handle():
    hndl = cdb2.Handle('mattdb', 'dev').execute("select 1 union select 2")


def test_commit_on_unused_connection():
    hndl = cdb2.Handle('mattdb', 'dev')
    with pytest.raises(cdb2.Error):
        hndl.execute("commit")


def test_empty_transactions():
    hndl = cdb2.Handle('mattdb', 'dev')
    hndl.execute("begin")
    hndl.execute("commit")

    hndl.execute("begin")
    hndl.execute("rollback")


def test_binding_parameters():
    hndl = cdb2.Handle('mattdb', 'dev')
    hndl.execute("insert into simple(key, val) values(@k, @v)", dict(v=2, k=1))
    hndl.execute("insert into simple(key, val) values(@k, @v)", dict(k=3, v=4))
    assert hndl.get_effects()[0] == 1

    rows = list(hndl.execute("select key, val from simple order by key"))
    assert rows == [[1,2],[3,4]]


def test_commit_failures():
    hndl = cdb2.Handle('mattdb', 'dev')
    hndl.execute("begin")
    hndl.execute("insert into simple(key, val) values(@k, @v)", dict(k=1, v=2))
    hndl.execute("insert into simple(key, val) values(@k, @v)", dict(k=3))

    try:
        hndl.execute("commit")
    except cdb2.Error as exc:
        assert exc.error_code == cdb2.ERROR_CODE['PREPARE_ERROR']
    else:
        assert False  # an assertion should have been raised!


def test_error_from_closing_connection_twice():
    hndl = cdb2.Handle('mattdb', 'dev')
    hndl.close()
    with pytest.raises(cdb2.Error):
        hndl.close()


def test_iterating_newly_initialized_handle():
    hndl = cdb2.Handle('mattdb', 'dev')
    for row in hndl:
        assert False


def test_timezone_handling():
    hndl = cdb2.Handle('mattdb', 'dev')
    rows = list(hndl.execute("select now()"))
    assert len(rows) == 1
    assert rows[0][0].tzname() == 'UTC'

    hndl = cdb2.Handle('mattdb', 'dev', tz='GMT')
    rows = list(hndl.execute("select now()"))
    assert len(rows) == 1
    assert rows[0][0].tzname() == 'GMT'

    hndl.execute("set timezone UTC")
    rows = list(hndl.execute("select now()"))
    assert len(rows) == 1
    assert rows[0][0].tzname() == 'UTC'


def test_passing_flags():
    # Bad SQL statements in a transaction fail early
    flags = cdb2.HANDLE_FLAGS['READ_INTRANS_RESULTS']

    # Test passing flags
    hndl = cdb2.Handle('mattdb', 'dev', flags=flags)
    hndl.execute('begin')
    with pytest.raises(cdb2.Error):
        hndl.execute('foobar')

    # Default behavior, without any flags passed
    hndl = cdb2.Handle('mattdb', 'dev', flags=0)
    hndl.execute('begin')
    hndl.execute('foobar')
    with pytest.raises(cdb2.Error):
        hndl.execute("select 1")


def test_row_factories():
    query = "select 1 as 'a', 2 as 'b' union select 3, 4 order by a"
    hndl = cdb2.Handle('mattdb', 'dev')

    assert list(hndl.execute(query)) == [[1, 2], [3, 4]]

    hndl.row_factory = dict_row_factory
    assert hndl.row_factory == dict_row_factory
    assert list(hndl.execute(query)) == [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]

    hndl.row_factory = namedtuple_row_factory
    assert hndl.row_factory == namedtuple_row_factory
    rows = [r._asdict() for r in hndl.execute(query)]
    assert rows == [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]


def test_row_factories_with_dup_col_names():
    query = "select 1 as 'a', 2 as 'a'"
    hndl = cdb2.Handle('mattdb', 'dev')

    assert list(hndl.execute(query)) == [[1, 2]]

    hndl.row_factory = namedtuple_row_factory
    with pytest.raises(cdb2.Error):
        hndl.execute(query)

    hndl.row_factory = dict_row_factory
    with pytest.raises(cdb2.Error):
        hndl.execute(query)


def test_failures_instantiating_row_class():
    def factory(col_names):
        def klass(col_values):
            raise TypeError(col_values)
        return klass

    query = "select 1 as 'a', 2 as 'a'"

    hndl = cdb2.Handle('mattdb', 'dev')
    hndl.row_factory = factory
    it = iter(hndl.execute(query))
    with pytest.raises(cdb2.Error):
        next(it)
