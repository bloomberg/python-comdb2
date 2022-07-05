import pytest

from comdb2 import dbapi2
from comdb2 import factories

MONITOR_PROCEDURE = """
local function main()
    db:column_type('int', 1)
    db:column_name('id', 1)

    db:column_type('text', 2)
    db:column_name('old_message', 2)

    db:column_type('text', 3)
    db:column_name('new_message', 3)

    local consumer = db:consumer()
    while true do
        local change = consumer:get()
        local row = {id=nil, old_message=nil, new_message=nil}
        if change.new ~= nil then
            row.id = change.new.id
            row.new_message = change.new.message
        end
        if change.old ~= nil then
            row.id = change.old.id
            row.old_message = change.old.message
        end

        consumer:emit(row)
        consumer:consume()
    end
end
"""


@pytest.fixture(autouse=True)
def create_consumer():
    conn = dbapi2.Connection("mattdb", "dev", autocommit=True)
    cursor = conn.cursor()

    # Drop existing consumer/procedure/table (just in case)
    try:
        cursor.execute("DROP LUA CONSUMER monitor")
    except dbapi2.Error:
        pass

    try:
        cursor.execute("DROP PROCEDURE monitor VERSION 'test'")
    except dbapi2.Error:
        pass

    cursor.execute("DROP TABLE IF EXISTS monitored")

    # (Re)create table/procedure/consumer
    cursor.execute("CREATE TABLE monitored(id int, message vutf8)")
    cursor.execute(
        "CREATE PROCEDURE monitor VERSION 'test' { " + MONITOR_PROCEDURE + "}"
    )
    cursor.execute(
        "CREATE LUA CONSUMER monitor"
        " ON (TABLE monitored FOR INSERT AND UPDATE AND DELETE)"
    )

    yield

    cursor.execute("DROP LUA CONSUMER monitor")
    cursor.execute("DROP PROCEDURE monitor VERSION 'test'")
    cursor.execute("DROP TABLE monitored")


def test_redelivery_after_gc_without_close():
    # GIVEN
    conn = dbapi2.Connection("mattdb", "dev", autocommit=True)
    conn.row_factory = factories.dict_row_factory
    cursor = conn.cursor()
    cursor.execute("insert into monitored(id, message) values (1, 'hi')")
    cursor.execute("insert into monitored(id, message) values (2, 'hey')")
    cursor.execute("insert into monitored(id, message) values (3, 'bye')")

    cursor.execute("exec procedure monitor()")
    assert cursor.fetchone() == {"id": 1, "old_message": None, "new_message": "hi"}
    assert cursor.fetchone() == {"id": 2, "old_message": None, "new_message": "hey"}
    cursor = None
    conn = None

    conn = dbapi2.Connection("mattdb", "dev", autocommit=True)
    conn.row_factory = factories.dict_row_factory
    cursor = conn.cursor()

    # WHEN
    cursor.execute("exec procedure monitor()")

    # THEN
    assert cursor.fetchone() == {"id": 2, "old_message": None, "new_message": "hey"}


def test_redelivery_after_close_without_consume():
    # GIVEN
    conn = dbapi2.Connection("mattdb", "dev", autocommit=True)
    conn.row_factory = factories.dict_row_factory
    cursor = conn.cursor()
    cursor.execute("insert into monitored(id, message) values (1, 'hi')")
    cursor.execute("insert into monitored(id, message) values (2, 'hey')")
    cursor.execute("insert into monitored(id, message) values (3, 'bye')")

    cursor.execute("exec procedure monitor()")
    assert cursor.fetchone() == {"id": 1, "old_message": None, "new_message": "hi"}
    assert cursor.fetchone() == {"id": 2, "old_message": None, "new_message": "hey"}
    cursor = None
    conn.close(ack_current_event=False)
    conn = None

    conn = dbapi2.Connection("mattdb", "dev", autocommit=True)
    conn.row_factory = factories.dict_row_factory
    cursor = conn.cursor()

    # WHEN
    cursor.execute("exec procedure monitor()")

    # THEN
    assert cursor.fetchone() == {"id": 2, "old_message": None, "new_message": "hey"}


def test_no_redelivery_after_default_close():
    # GIVEN
    conn = dbapi2.Connection("mattdb", "dev", autocommit=True)
    conn.row_factory = factories.dict_row_factory
    cursor = conn.cursor()
    cursor.execute("insert into monitored(id, message) values (1, 'hi')")
    cursor.execute("insert into monitored(id, message) values (2, 'hey')")
    cursor.execute("insert into monitored(id, message) values (3, 'bye')")

    cursor.execute("exec procedure monitor()")
    assert cursor.fetchone() == {"id": 1, "old_message": None, "new_message": "hi"}
    assert cursor.fetchone() == {"id": 2, "old_message": None, "new_message": "hey"}
    cursor = None
    conn.close()
    conn = None

    conn = dbapi2.Connection("mattdb", "dev", autocommit=True)
    conn.row_factory = factories.dict_row_factory
    cursor = conn.cursor()

    # WHEN
    cursor.execute("exec procedure monitor()")

    # THEN
    assert cursor.fetchone() == {"id": 3, "old_message": None, "new_message": "bye"}
