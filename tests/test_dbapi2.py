# Copyright 2017 Bloomberg Finance L.P.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import unicode_literals, absolute_import

from comdb2.dbapi2 import NUMBER
from comdb2.dbapi2 import BINARY
from comdb2.dbapi2 import STRING
from comdb2.dbapi2 import DATETIME
from comdb2.dbapi2 import connect
from comdb2.dbapi2 import Binary
from comdb2.dbapi2 import Datetime
from comdb2.dbapi2 import DatetimeUs
from comdb2.dbapi2 import Timestamp
from comdb2.dbapi2 import TimestampUs
from comdb2.dbapi2 import DataError
from comdb2.dbapi2 import OperationalError
from comdb2.dbapi2 import IntegrityError
from comdb2.dbapi2 import ForeignKeyConstraintError
from comdb2.dbapi2 import NonNullConstraintError
from comdb2.dbapi2 import UniqueKeyConstraintError
from comdb2.dbapi2 import InterfaceError
from comdb2.dbapi2 import NotSupportedError
from comdb2.dbapi2 import ProgrammingError
from comdb2 import cdb2
from comdb2.factories import dict_row_factory
from comdb2.factories import namedtuple_row_factory
import pytest
import datetime
import pytz
import six
from functools import partial

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

COLUMN_LIST = ("short_col u_short_col int_col u_int_col longlong_col"
               " float_col double_col byte_col byte_array_col"
               " cstring_col pstring_col blob_col datetime_col vutf8_col"
               ).split()

COLUMN_TYPE = (NUMBER, NUMBER, NUMBER, NUMBER, NUMBER,
               NUMBER, NUMBER, BINARY, BINARY,
               STRING, STRING, BINARY, DATETIME, STRING)


@pytest.fixture(autouse=True)
def delete_all_rows():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    while True:
        cursor.execute("delete from all_datatypes limit 100")
        conn.commit()
        if cursor.rowcount != 100:
            break
    while True:
        cursor.execute("delete from simple limit 100")
        conn.commit()
        if cursor.rowcount != 100:
            break
    while True:
        cursor.execute("delete from strings limit 100")
        conn.commit()
        if cursor.rowcount != 100:
            break
    conn.close()


def test_invalid_cluster():
    with pytest.raises(OperationalError):
        connect('mattdb', 'foo')


def test_invalid_dbname():
    with pytest.raises(OperationalError):
        connect('', 'dev')


def test_closing_unused_connection():
    conn = connect('mattdb', 'dev')
    conn.close()


def test_garbage_collecting_unused_connection():
    conn = connect('mattdb', 'dev')
    del conn


def test_commit_on_unused_connection():
    conn = connect('mattdb', 'dev')
    conn.commit()


def test_rollback_on_unused_connection():
    conn = connect('mattdb', 'dev')
    conn.rollback()


def test_inserts():
    # Without params
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("insert into simple(key, val) values(1, 2)")
    cursor.connection.commit()
    assert cursor.rowcount == 1

    cursor.execute("select key, val from simple order by key")
    assert cursor.fetchall() == [[1, 2]]

    # With params
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("insert into simple(key, val) values(%(k)s, %(v)s)",
                   dict(k=3, v=4))
    conn.commit()
    assert cursor.rowcount == 1

    cursor.execute("select key, val from simple order by key")
    assert cursor.fetchall() == [[1, 2], [3, 4]]


def test_rollback():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("insert into simple(key, val) values(1, 2)")
    conn.rollback()

    cursor.execute("select key, val from simple order by key")
    assert cursor.fetchall() == []


def test_commit_failures():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.executemany("insert into simple(key, val) values(%(key)s, %(val)s)",
                       [ dict(key=1, val=2), dict(key=3, val=None) ])
    with pytest.raises(IntegrityError):
        conn.commit()
        assert cursor.rowcount == 0

    cursor.execute("select * from simple")
    assert cursor.fetchall() == []


def test_unique_key_violation():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("insert into simple(key, val) values(1, 2)")
    conn.commit()
    cursor.execute("insert into simple(key, val) values(1, 3)")
    with pytest.raises(IntegrityError):
        conn.commit()


def test_constraint_errors():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()

    cursor.execute("insert into simple(key, val) values(1, 2)")
    cursor.execute("insert into simple(key, val) values(1, 2)")
    with pytest.raises(UniqueKeyConstraintError) as exc_info:
        conn.commit()
    errcode = ' (cdb2api rc %d)' % cdb2.ERROR_CODE['DUPLICATE']
    assert errcode in str(exc_info.value)

    cursor.execute("insert into simple(key, val) values(null, 2)")
    with pytest.raises(NonNullConstraintError) as exc_info:
        conn.commit()
    errcode = ' (cdb2api rc %d)' % cdb2.ERROR_CODE['NULL_CONSTRAINT']
    assert errcode in str(exc_info.value)

    cursor.execute("insert into simple(key, val) values(1, 2)")
    conn.commit()

    cursor.execute("selectv * from simple")
    connect('mattdb', 'dev', autocommit=True).cursor().execute(
        "update simple set key=2")
    with pytest.raises(IntegrityError) as exc_info:
        conn.commit()
    errcode = ' (cdb2api rc %d)' % cdb2.ERROR_CODE['CONSTRAINTS']
    assert errcode in str(exc_info.value)

    cursor.execute("insert into child(key) values(1)")
    with pytest.raises(ForeignKeyConstraintError) as exc_info:
        conn.commit()
    errcode = ' (cdb2api rc %d)' % cdb2.ERROR_CODE['FKEY_VIOLATION']
    assert errcode in str(exc_info.value)


def test_implicitly_closing_old_cursor_when_opening_a_new_one():
    conn = connect('mattdb', 'dev')
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    with pytest.raises(InterfaceError):
        assert cursor1.rowcount == -1
    assert cursor2.rowcount == -1


def test_commit_after_cursor_close():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("insert into simple(key, val) values(1, 2)")
    conn.commit()
    cursor.execute("insert into simple(key, val) values(3, 4)")
    cursor.close()
    conn.commit()

    cursor = conn.cursor()
    cursor.execute("select key, val from simple order by key")
    assert cursor.fetchall() == [[1, 2], [3, 4]]


def test_implicit_rollback_on_connection_close():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("insert into simple(key, val) values(1, 2)")
    conn.commit()
    cursor.execute("insert into simple(key, val) values(3, 4)")

    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    rows = list(cursor.execute("select key, val from simple order by key"))
    assert rows == [[1, 2]]


def test_extra_percent_arg():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    with pytest.raises(InterfaceError):
        cursor.execute("insert into simple(key, val) values(%(k)s, %(v)s)",
                       dict(k=3))


def test_unescaped_percent():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("select 1%%2" )  # Should work
    with pytest.raises(InterfaceError):
        cursor.execute("select 1%2")


def test_reading_and_writing_datetimes():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    ts_obj = Timestamp(2015, 1, 2, 3, 4, 5, 123000, pytz.UTC)
    ts_str_in = '2015-01-02T03:04:05.12345'
    ts_str_out = '2015-01-02T030405.123 UTC'

    cursor.execute("select cast(%(x)s as date)", dict(x=ts_str_in))
    assert cursor.fetchall() == [[ts_obj]]

    cursor.execute("select %(x)s || ''", dict(x=ts_obj))
    assert cursor.fetchall() == [[ts_str_out]]


def test_reading_and_writing_datetimeus():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    ts_obj = TimestampUs(2015, 1, 2, 3, 4, 5, 123456, pytz.UTC)
    ts_str_in = '2015-01-02T03:04:05.123456'
    ts_str_out = '2015-01-02T030405.123456 UTC'

    cursor.execute("select cast(%(x)s as date)", dict(x=ts_str_in))
    assert cursor.fetchall() == [[ts_obj]]

    cursor.execute("select %(x)s || ''", dict(x=ts_obj))
    assert cursor.fetchall() == [[ts_str_out]]


def test_inserting_one_row_with_all_datatypes_without_parameters():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("insert into all_datatypes(" + ', '.join(COLUMN_LIST) + ")"
                   " values(1, 2, 3, 4, 5, .5, .25, x'01', x'0102030405',"
                          " 'hello', 'goodbye', x'01020304050607',"
                          " cast(1234567890.2345 as datetime), 'hello world')")
    conn.commit()
    assert cursor.rowcount == 1

    cursor.execute("select " + ', '.join(COLUMN_LIST) + " from all_datatypes")

    for i in range(len(COLUMN_LIST)):
        assert cursor.description[i][0] == COLUMN_LIST[i]
        for type_object in (STRING, BINARY, NUMBER, DATETIME):
            assert ((type_object == cursor.description[i][1])
                    == (type_object == COLUMN_TYPE[i]))
        assert cursor.description[i][2:] == (None, None, None, None, None)

    row = cursor.fetchone()
    assert row == [1, 2, 3, 4, 5, 0.5, 0.25,
                   Binary('\x01'), Binary('\x01\x02\x03\x04\x05'),
                   'hello', 'goodbye',
                   Binary('\x01\x02\x03\x04\x05\x06\x07'),
                   pytz.timezone("America/New_York").localize(
                        Datetime(2009, 2, 13, 18, 31, 30, 234000)),
                   "hello world"]
    assert cursor.fetchone() is None


def test_all_datatypes_as_parameters():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    params = (
        ("short_col", 32767),
        ("u_short_col", 65535),
        ("int_col", 2147483647),
        ("u_int_col", 4294967295),
        ("longlong_col", 9223372036854775807),
        ("float_col", .125),
        ("double_col", 2.**65),
        ("byte_col", Binary(b'\x00')),
        ("byte_array_col", Binary(b'\x02\x01\x00\x01\x02')),
        ("cstring_col", 'HELLO'),
        ("pstring_col", 'GOODBYE'),
        ("blob_col", Binary('')),
        ("datetime_col", pytz.timezone("America/New_York").localize(
                              Datetime(2009, 2, 13, 18, 31, 30, 234000))),
        ("vutf8_col", "foo" * 50)
    )
    cursor.execute("insert into all_datatypes(" + ', '.join(COLUMN_LIST) + ")"
                   " values(%(short_col)s, %(u_short_col)s, %(int_col)s,"
                   " %(u_int_col)s, %(longlong_col)s, %(float_col)s,"
                   " %(double_col)s, %(byte_col)s, %(byte_array_col)s,"
                   " %(cstring_col)s, %(pstring_col)s, %(blob_col)s,"
                   " %(datetime_col)s, %(vutf8_col)s)", dict(params))

    conn.commit()

    cursor.execute("select * from all_datatypes")
    row = cursor.fetchone()
    assert row == list(v for k, v in params)
    assert cursor.fetchone() is None


def test_naive_datetime_as_parameter():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    params = (
        ("short_col", 32767),
        ("u_short_col", 65535),
        ("int_col", 2147483647),
        ("u_int_col", 4294967295),
        ("longlong_col", 9223372036854775807),
        ("float_col", .125),
        ("double_col", 2.**65),
        ("byte_col", Binary(b'\x00')),
        ("byte_array_col", Binary(b'\x02\x01\x00\x01\x02')),
        ("cstring_col", 'HELLO'),
        ("pstring_col", 'GOODBYE'),
        ("blob_col", Binary('')),
        ("datetime_col", Datetime(2009, 2, 13, 18, 31, 30, 234000)),
        ("vutf8_col", "foo" * 50)
    )

    cursor.execute("insert into all_datatypes(" + ', '.join(COLUMN_LIST) + ")"
                   " values(%(short_col)s, %(u_short_col)s, %(int_col)s,"
                   " %(u_int_col)s, %(longlong_col)s, %(float_col)s,"
                   " %(double_col)s, %(byte_col)s, %(byte_array_col)s,"
                   " %(cstring_col)s, %(pstring_col)s, %(blob_col)s,"
                   " %(datetime_col)s, %(vutf8_col)s)", dict(params))

    cursor.connection.commit()
    cursor.execute("select datetime_col from all_datatypes")
    row = cursor.fetchone()
    assert row == [Datetime(2009, 2, 13, 18, 31, 30, 234000, pytz.UTC)]
    assert cursor.fetchone() is None


def test_datetime_with_non_olson_tzname():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    tz = pytz.timezone('America/New_York')
    dt = datetime.datetime(2016, 11, 6, 1, 30, 0, 123000)
    est_dt = tz.localize(dt, is_dst=False)
    edt_dt = tz.localize(dt, is_dst=True)
    assert est_dt.tzname() == 'EST'
    assert edt_dt.tzname() == 'EDT'
    params = {'est_dt': est_dt, 'edt_dt': edt_dt}
    row = cursor.execute("select @est_dt, @edt_dt", params).fetchone()
    assert row[0].tzname() == 'UTC'
    assert row[0] == est_dt
    assert row[1].tzname() == 'UTC'
    assert row[1] == edt_dt


def test_rounding_datetime_to_nearest_millisecond():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()

    curr_microsecond = Datetime(2016, 2, 28, 23, 59, 59, 999499, pytz.UTC)
    prev_millisecond = Datetime(2016, 2, 28, 23, 59, 59, 999000, pytz.UTC)
    next_millisecond = Datetime(2016, 2, 29, 0, 0, 0, 0, pytz.UTC)

    cursor.execute("select @date", {'date': curr_microsecond})
    assert cursor.fetchall() == [[prev_millisecond]]

    curr_microsecond += datetime.timedelta(microseconds=1)
    cursor.execute("select @date", {'date': curr_microsecond})
    assert cursor.fetchall() == [[next_millisecond]]


def test_cursor_description():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    assert cursor.description is None

    cursor.execute("select 1")
    assert cursor.description == (("1", NUMBER, None, None, None, None, None),)

    cursor.execute("select 1 where 1=0")
    assert len(cursor.description) == 1
    assert cursor.description[0][0] == "1"

    cursor.execute("select '1' as foo, cast(1 as datetime) bar")
    assert cursor.description == (
        ("foo", STRING, None, None, None, None, None),
        ("bar", DATETIME, None, None, None, None, None))

    cursor.connection.commit()
    assert cursor.description == (
        ("foo", STRING, None, None, None, None, None),
        ("bar", DATETIME, None, None, None, None, None))

    cursor.execute("insert into simple(key, val) values(3, 4)")
    assert cursor.description is None

    cursor.connection.commit()
    assert cursor.description is None

    cursor.execute("select key, val from simple")
    assert cursor.description == (
        ("key", NUMBER, None, None, None, None, None),
        ("val", NUMBER, None, None, None, None, None))

    cursor.connection.rollback()
    assert cursor.description == (
        ("key", NUMBER, None, None, None, None, None),
        ("val", NUMBER, None, None, None, None, None))

    with pytest.raises(ProgrammingError):
        cursor.execute("select")
    assert cursor.description is None


def test_binding_number_that_overflows_long_long():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    with pytest.raises(DataError):
        cursor.execute("select @i", dict(i=2**64 + 1))


def test_retrieving_null():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("select null, null")
    assert cursor.fetchall() == [[None, None]]


def test_retrieving_interval():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("select cast(0 as datetime) - cast(0 as datetime)")
    with pytest.raises(NotSupportedError):
        cursor.fetchone()


def test_syntax_error():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("foo")
    with pytest.raises(ProgrammingError):
        conn.rollback()


def test_errors_being_swallowed_during_cursor_close():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("foo")
    cursor.close()


def test_errors_being_swallowed_during_connection_close():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("foo")
    conn.close()


def test_public_connection_methods_after_close():
    conn = connect('mattdb', 'dev')
    conn.close()
    with pytest.raises(InterfaceError):
        conn.close()
    with pytest.raises(InterfaceError):
        conn.commit()
    with pytest.raises(InterfaceError):
        conn.cursor()
    with pytest.raises(InterfaceError):
        conn.rollback()
    with pytest.raises(InterfaceError):
        conn.row_factory
    with pytest.raises(InterfaceError):
        conn.row_factory = None


def test_misusing_cursor_objects():
    with pytest.raises(InterfaceError):
        conn = connect('mattdb', 'dev')
        cursor = conn.cursor()
        cursor.execute(" BEGIN TRANSACTION ")

    with pytest.raises(InterfaceError):
        conn = connect('mattdb', 'dev')
        cursor = conn.cursor()
        cursor.execute("commit")

    with pytest.raises(InterfaceError):
        conn = connect('mattdb', 'dev')
        cursor = conn.cursor()
        cursor.execute("  rollback   ")

    with pytest.raises(InterfaceError):
        conn = connect('mattdb', 'dev')
        cursor = conn.cursor()
        cursor.fetchone()

    with pytest.raises(InterfaceError):
        conn = connect('mattdb', 'dev')
        cursor = conn.cursor()
        cursor.fetchmany()

    with pytest.raises(InterfaceError):
        conn = connect('mattdb', 'dev')
        cursor = conn.cursor()
        cursor.fetchall()


def test_noop_methods():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.setinputsizes([1, 2, 3])
    cursor.setoutputsize(42)


def test_fetchmany():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()

    cursor.execute("select 1 UNION select 2 UNION select 3 order by 1")
    assert cursor.fetchmany() == [[1]]
    assert cursor.fetchmany(2) == [[2], [3]]
    assert cursor.fetchmany(2) == []
    assert cursor.fetchmany(2) == []

    cursor.arraysize = 2
    assert cursor.arraysize == 2
    cursor.execute("select 1 UNION select 2 UNION select 3 order by 1")
    assert cursor.fetchmany() == [[1], [2]]
    assert cursor.fetchmany() == [[3]]

    cursor.arraysize = 4
    assert cursor.arraysize == 4
    cursor.execute("select 1 UNION select 2 UNION select 3 order by 1")
    assert cursor.fetchmany() == [[1], [2], [3]]


def test_consuming_result_sets_automatically():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("select 1 UNION select 2 UNION select 3 order by 1")
    cursor.execute("select 1 UNION select 2 UNION select 3 order by 1")


def test_inserting_non_utf8_string():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()

    cursor.execute("insert into strings values(cast(%(x)s as text), %(y)s)",
                   dict(x=b'\x68\xeb\x6c\x6c\x6f', y=b'\x68\xeb\x6c\x6c\x6f'))
    conn.commit()

    with pytest.raises(DataError):
        cursor.execute("select foo, bar from strings")
        rows = list(cursor)

    rows = list(cursor.execute("select cast(foo as blob), bar from strings"))
    assert rows == [[b'\x68\xeb\x6c\x6c\x6f', b'\x68\xeb\x6c\x6c\x6f']]


def test_cursor_connection_attribute_keeps_connection_alive():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    del conn
    cursor.execute("insert into simple(key, val) values(1, 2)")
    cursor.connection.commit()
    assert cursor.rowcount == 1

    cursor.execute("select key, val from simple order by key")
    assert cursor.fetchall() == [[1, 2]]


def test_exceptions_containing_unicode_error_messages():
    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    with pytest.raises(ProgrammingError):
        try:
            cursor.execute("select")
        except ProgrammingError as exc:
            assert isinstance(exc.args[0], six.text_type)
            raise


def throw_on(expected_stmt, stmt, parameters=None):
    if stmt == expected_stmt:
        raise cdb2.Error(42, 'Not supported error')


@patch('comdb2.cdb2.Handle')
def test_begin_throws_error(handle):
    handle.return_value.execute.side_effect = partial(throw_on, 'begin')

    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()

    with pytest.raises(OperationalError):
        cursor.execute("insert into simple(key, val) values(1, 2)")


@patch('comdb2.cdb2.Handle')
def test_commit_throws_error(handle):
    handle.return_value.execute.side_effect = partial(throw_on, 'commit')

    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("insert into simple(key, val) values(1, 2)")

    with pytest.raises(OperationalError):
        conn.commit()


@patch('comdb2.cdb2.Handle')
def test_get_effect_throws_error(handle):
    def raise_not_supported_error():
        raise cdb2.Error(42, 'Not supported error')

    handle.return_value.get_effects.side_effect = raise_not_supported_error

    conn = connect('mattdb', 'dev')
    cursor = conn.cursor()
    cursor.execute("insert into simple(key, val) values(1, 2)")
    conn.commit()


def test_autocommit_handles():
    conn = connect('mattdb', 'dev', autocommit=True)
    cursor = conn.cursor()

    # Explicit transactions must work
    cursor.execute("begin")
    assert cursor.rowcount == -1
    cursor.execute("insert into simple(key, val) values(1, 2)")
    assert cursor.rowcount == -1
    cursor.execute("insert into simple(key, val) values(3, 4)")
    assert cursor.rowcount == -1
    cursor.execute("commit")
    assert cursor.rowcount == 2

    # Selects work, but don't affect the rowcount
    cursor.execute("select key, val from simple order by key")
    assert cursor.rowcount == -1
    assert cursor.fetchall() == [[1, 2], [3, 4]]

    cursor.execute("selectv key, val from simple order by key")
    assert cursor.rowcount == -1
    assert cursor.fetchall() == [[1, 2], [3, 4]]

    # Outside of a transaction, operations are applied immediately
    cursor.execute("insert into simple(key, val) values(7, 6)")
    assert cursor.rowcount == 1

    cursor.execute("update simple set key=5 where key=7")
    assert cursor.rowcount == 1

    cursor.execute("delete from simple where key in (1, 3, 5)")
    assert cursor.rowcount == 3

    cursor.execute("select count(*) from simple")
    assert cursor.fetchall() == [[0]]

    # Outside of a transaction, commit fails
    with pytest.raises(ProgrammingError):
        conn.commit()

    # The rowcount isn't updated on rollback
    cursor.execute("begin")
    cursor.execute("insert into simple(key, val) values(1, 2)")
    conn.rollback()
    assert cursor.rowcount == -1


def test_row_factories():
    query = "select 1 as 'a', 2 as 'b' union select 3, 4 order by a"
    hndl = connect('mattdb', 'dev')

    assert list(hndl.cursor().execute(query)) == [[1, 2], [3, 4]]

    hndl.row_factory = dict_row_factory
    assert hndl.row_factory == dict_row_factory
    rows = list(hndl.cursor().execute(query))
    assert rows == [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]

    hndl.row_factory = namedtuple_row_factory
    assert hndl.row_factory == namedtuple_row_factory
    rows = [r._asdict() for r in hndl.cursor().execute(query)]
    assert rows == [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]


def test_row_factories_with_dup_col_names():
    query = "select 1 as 'a', 2 as 'a', 3 as 'b', 4 as 'b', 5 as 'c'"
    hndl = connect('mattdb', 'dev')

    assert list(hndl.cursor().execute(query)) == [[1, 2, 3, 4, 5]]

    hndl.row_factory = namedtuple_row_factory
    with pytest.raises(OperationalError):
        hndl.cursor().execute(query)

    hndl.row_factory = dict_row_factory
    with pytest.raises(OperationalError):
        hndl.cursor().execute(query)


def test_row_factories_with_reserved_word_col_names():
    query = "select 1 as 'def'"
    hndl = connect('mattdb', 'dev')

    assert list(hndl.cursor().execute(query)) == [[1]]

    hndl.row_factory = namedtuple_row_factory
    with pytest.raises(OperationalError):
        hndl.cursor().execute(query)

    hndl.row_factory = dict_row_factory
    assert list(hndl.cursor().execute(query)) == [{'def': 1}]


def test_reusing_handle_after_unicode_decode_error():
    hndl = connect('mattdb', 'dev')
    cursor = hndl.cursor()
    with pytest.raises(DataError):
        cursor.execute("select cast(X'C3' as text)").fetchall()
    row = cursor.execute("select cast(X'C3A4' as text)").fetchone()
    assert row == ['\xE4']


def test_unicode_column_decode_exception():
    query = "select cast(X'C3A4' as text) as a, cast(X'C3' as text) as b"
    hndl = connect('mattdb', 'dev')
    cursor = hndl.cursor()

    cursor.execute(query)
    with pytest.raises(DataError) as exc_info:
        cursor.fetchall()

    errmsg = "Failed to decode CDB2_CSTRING column 1 ('b'):"
    assert errmsg in str(exc_info.value)

    errmsg = "can't decode byte 0xc3 in position 0: unexpected end of data"
    assert errmsg in str(exc_info.value)


def test_date_column_decode_exception():
    query = "select cast('0000-01-01 UTC' as date) as date"
    hndl = connect('mattdb', 'dev')
    cursor = hndl.cursor()

    cursor.execute("SET TIMEZONE America/New_York")
    cursor.execute(query)
    with pytest.raises(DataError) as exc_info:
        cursor.fetchall()

    exc_str = str(exc_info.value)
    assert "Failed to decode CDB2_DATETIME column 0 ('date'):" in exc_str
    assert " out of range" in exc_str


def test_unsupported_column_decode_exception():
    query = "select now() - now() as delta"
    hndl = connect('mattdb', 'dev')
    cursor = hndl.cursor()

    cursor.execute(query)
    with pytest.raises(NotSupportedError) as exc_info:
        cursor.fetchall()

    errmsg = ("Failed to decode CDB2_INTERVALDS column 0 ('delta'):"
              " Unsupported column type")
    assert errmsg in str(exc_info.value)


def test_datetimeus():
    query = "select %(date)s + cast(30 as days)"
    hndl = connect('mattdb', 'dev')
    cursor = hndl.cursor()

    sent = DatetimeUs(2017, 8, 16, 19, 32, 2, 825022, tzinfo=pytz.UTC)
    cursor.execute(query, dict(date=sent))
    rcvd = cursor.fetchall()[0][0]

    assert sent + datetime.timedelta(days=30) == rcvd


def test_interface_error_reading_result_set_after_commits():
    hndl = connect('mattdb', 'dev')
    cursor = hndl.cursor().execute("delete from simple where 1=1")
    assert cursor.description is None
    hndl.commit()
    with pytest.raises(InterfaceError) as exc_info:
        cursor.fetchall()
    assert "No result set exists" in str(exc_info.value)

    hndl = connect('mattdb', 'dev', autocommit=True)
    cursor = hndl.cursor().execute("delete from simple where 1=1")
    assert cursor.description is None
    with pytest.raises(InterfaceError) as exc_info:
        cursor.fetchall()
    assert "No result set exists" in str(exc_info.value)

def test_context_manager_support():
    with connect('mattdb', 'dev') as hndl1:
       pass

    assert hndl1.isOpen() == False

    with pytest.raises(Exception) as exc_info:
        with connect('mattdb', 'dev') as hndl2:
            raise Exception('bad')

    assert hndl2.isOpen() == False
