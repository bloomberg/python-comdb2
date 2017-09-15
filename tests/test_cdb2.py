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

from comdb2 import cdb2
from comdb2.factories import dict_row_factory
from comdb2.factories import namedtuple_row_factory
import pytest
import six

COLUMN_LIST = ("short_col u_short_col int_col u_int_col longlong_col"
               " float_col double_col byte_col byte_array_col"
               " cstring_col pstring_col blob_col datetime_col vutf8_col"
               ).split()


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
    cdb2.Handle('mattdb', 'dev').execute("select 1 union select 2")


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
    assert rows == [[1, 2], [3, 4]]


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
    with pytest.raises(cdb2.Error) as exc_info:
        hndl.execute(query)
    assert isinstance(exc_info.value.args[1], six.text_type)

    hndl.row_factory = dict_row_factory
    with pytest.raises(cdb2.Error) as exc_info:
        hndl.execute(query)
    assert isinstance(exc_info.value.args[1], six.text_type)


def test_failures_instantiating_row_class():
    def factory(col_names):
        def klass(col_values):
            raise TypeError(col_values)
        return klass

    query = "select 1 as 'a', 2 as 'a'"

    hndl = cdb2.Handle('mattdb', 'dev')
    hndl.row_factory = factory
    it = iter(hndl.execute(query))
    with pytest.raises(cdb2.Error) as exc_info:
        next(it)
    assert isinstance(exc_info.value.args[1], six.text_type)


def test_get_effects():
    hndl = cdb2.Handle('mattdb', 'dev')

    hndl.execute("select 1 union all select 2")
    assert hndl.get_effects() == (0, 2, 0, 0, 0)
    assert hndl.get_effects().num_selected == 2

    hndl.execute("insert into simple(key, val) values(@k, @v)", dict(v=2, k=1))
    assert hndl.get_effects() == (1, 0, 0, 0, 1)
    assert hndl.get_effects().num_affected == 1
    assert hndl.get_effects().num_inserted == 1

    hndl.execute("update simple set val=3 where key=1")
    assert hndl.get_effects() == (1, 0, 1, 0, 0)
    assert hndl.get_effects().num_affected == 1
    assert hndl.get_effects().num_updated == 1

    hndl.execute("delete from simple where 1=1")
    assert hndl.get_effects() == (1, 0, 0, 1, 0)
    assert hndl.get_effects().num_affected == 1
    assert hndl.get_effects().num_deleted == 1


def test_nonascii_error_messages():
    with pytest.raises(cdb2.Error) as exc_info:
        cdb2.Handle('mattdb', b'\xc3')
    assert r'cluster type \xc3.' in exc_info.value.error_message

    hndl = cdb2.Handle('mattdb', 'dev')
    with pytest.raises(cdb2.Error) as exc_info:
        hndl.execute(b'select \xC3')
    assert r'no such column: \xc3' in exc_info.value.error_message

    teststr = bytes(bytearray(range(128, 256)))
    with pytest.raises(cdb2.Error) as exc_info:
        hndl.execute(b'select ' + teststr)
    assert (
        r'no such column: '
        r'\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f'
        r'\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f'
        r'\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf'
        r'\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf'
        r'\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf'
        r'\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf'
        r'\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef'
        r'\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff'
          in exc_info.value.error_message)

    teststr = bytes(bytearray(range(1, 31)))
    with pytest.raises(cdb2.Error) as exc_info:
        cdb2.Handle('mattdb', teststr)
    errmsg_utf8 = exc_info.value.error_message.encode('utf-8')
    assert teststr in errmsg_utf8

    teststr = bytes(bytearray(range(1, 31))) + b'\xff'
    with pytest.raises(cdb2.Error) as exc_info:
        cdb2.Handle('mattdb', teststr)
    errmsg_utf8 = exc_info.value.error_message.encode('utf-8')
    assert teststr not in errmsg_utf8
    assert teststr.decode('latin1').encode('unicode_escape') in errmsg_utf8


def test_namedtuple_factory_dml():
    hndl = cdb2.Handle('mattdb', 'dev')
    hndl.row_factory = namedtuple_row_factory

    hndl.execute("insert into simple (key, val) values (1, 1)")
    assert next(hndl)._0 == 1

    hndl.execute("update simple set val=0 where key=1")
    assert next(hndl)._0 == 1

    hndl.execute("delete from simple where 1=1")
    assert next(hndl)._0 == 1
