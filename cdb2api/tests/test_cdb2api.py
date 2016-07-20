from .. import cdb2api
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
    hndl = cdb2api.Handle('mattdb', 'dev')
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
    hndl = cdb2api.Handle('mattdb', 'dev').execute("select 1 union select 2")


def test_commit_on_unused_connection():
    hndl = cdb2api.Handle('mattdb', 'dev')
    with pytest.raises(cdb2api.Error):
        hndl.execute("commit")


def test_empty_transactions():
    hndl = cdb2api.Handle('mattdb', 'dev')
    hndl.execute("begin")
    hndl.execute("commit")

    hndl.execute("begin")
    hndl.execute("rollback")


def test_binding_parameters():
    hndl = cdb2api.Handle('mattdb', 'dev')
    hndl.execute("insert into simple(key, val) values(@k, @v)", dict(v=2, k=1))
    hndl.execute("insert into simple(key, val) values(@k, @v)", dict(k=3, v=4))
    assert hndl.get_effects()[0] == 1

    rows = list(hndl.execute("select key, val from simple order by key"))
    assert rows == [(1,2),(3,4)]


def test_commit_failures():
    hndl = cdb2api.Handle('mattdb', 'dev')
    hndl.execute("begin")
    hndl.execute("insert into simple(key, val) values(@k, @v)", dict(k=1, v=2))
    hndl.execute("insert into simple(key, val) values(@k, @v)", dict(k=3))

    try:
        hndl.execute("commit")
    except cdb2api.Error as exc:
        pass

    assert exc.error_code == cdb2api.ERROR_CODE['PREPARE_ERROR']


def test_error_from_closing_connection_twice():
    hndl = cdb2api.Handle('mattdb', 'dev')
    hndl.close()
    with pytest.raises(cdb2api.Error):
        hndl.close()


def test_iterating_newly_initialized_handle():
    hndl = cdb2api.Handle('mattdb', 'dev')
    for row in hndl:
        assert False
