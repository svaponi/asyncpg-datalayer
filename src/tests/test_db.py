import uuid

import pytest

SQL_CREATE_FOOBAR = """
CREATE TABLE foobar (
    id          uuid PRIMARY KEY NOT NULL,
    name        character varying NOT NULL,
    email       character varying UNIQUE NOT NULL
)
"""


@pytest.mark.asyncio
async def test_db(db):
    await db.exec_ddl(SQL_CREATE_FOOBAR)

    foo_id = uuid.uuid4()
    result = await db.exec_dml(
        "insert into foobar(id, name, email) values(:foo_id,'foo','foo@example.com')",
        foo_id=foo_id,
    )
    assert result == 1

    bar_id = uuid.uuid4()
    result = await db.exec_dml(
        "insert into foobar(id, name, email) values(:bar_id,'bar','bar@example.com')",
        bar_id=bar_id,
    )
    assert result == 1

    result = await db.exec_sql("select id, name, email from foobar")
    assert len(result) == 2

    result = await db.exec_sql(
        "select id, name, email from foobar where name = :name", name="foo"
    )
    assert len(result) == 1
    assert result[0]["id"] == foo_id
    assert result[0]["name"] == "foo"
    assert result[0]["email"] == "foo@example.com"

    result = await db.exec_sql(
        "select id, name, email from foobar where name = :name", name="bar"
    )
    assert len(result) == 1
    assert result[0]["id"] == bar_id
    assert result[0]["name"] == "bar"
    assert result[0]["email"] == "bar@example.com"

    result = await db.exec_sql(
        "select id, name, email from foobar where name in :name",
        name=["foo", "bar"],
    )
    assert len(result) == 2
