import asyncio
import datetime
import time
import uuid

import pytest
import sqlalchemy
from sqlalchemy import Uuid, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from asyncpg_datalayer.db import DB


class _Base(DeclarativeBase):
    pass


class Foobar(_Base):
    __tablename__ = "foobar"
    foobar_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)


@pytest.mark.parametrize(
    "num_of_rows, readonly",
    [
        (10, False),
        (10, True),
        (100, False),
        (100, True),
        (1_000, False),
        (1_000, True),
        (5_000, False),
        (5_000, True),
    ],
)
@pytest.mark.asyncio
@pytest.mark.skip
async def test_db_perf(db, num_of_rows, readonly):
    async with db.connection() as conn:
        await conn.run_sync(_Base.metadata.create_all)
    print(f"\n---")
    await run_perf_test(db, num_of_rows, readonly=readonly, read_loops=10)


async def run_perf_test(db, num_of_rows, readonly, read_loops):
    mark0 = time.perf_counter()
    coroutines = [insert(db) for _ in range(num_of_rows)]
    foobar_ids = await asyncio.gather(*coroutines)
    mark1 = time.perf_counter()
    print(f"Inserted {num_of_rows} rows in {mark1 - mark0:0.4f} seconds")

    elapsed = 0
    mark0 = time.perf_counter()
    for _ in range(read_loops):
        elapsed += await run_read_test(db, foobar_ids, readonly=readonly)
    mark1 = time.perf_counter()
    elapsed /= read_loops
    elapsed_total = mark1 - mark0
    print(
        f"Retrieved {num_of_rows=} {elapsed_total=:0.6f} {elapsed=:0.6f} ({readonly=})"
    )


async def run_read_test(db, foobar_ids, readonly=None):
    mark0 = time.perf_counter()
    coroutines = [
        get_or_none_by_id(db, foobar_id, readonly=readonly) for foobar_id in foobar_ids
    ]
    foobars = await asyncio.gather(*coroutines)
    mark1 = time.perf_counter()
    assert {foobar.foobar_id for foobar in foobars} == set(foobar_ids)
    return mark1 - mark0


async def get_or_none_by_id(
    db: DB,
    entity_id: uuid.UUID,
    readonly: bool | None = None,
) -> Foobar | None:
    query = sqlalchemy.select(Foobar).where(Foobar.foobar_id == entity_id)
    async with db.get_session(readonly=readonly) as session:
        response = await session.execute(query)
        result = response.scalar_one_or_none()
    return result


async def insert(
    db: DB,
    reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
) -> uuid.UUID:
    insert_obj = {"foobar_id": uuid.uuid4(), "created_at": datetime.datetime.now()}
    query = sqlalchemy.insert(Foobar).values(**insert_obj).returning(Foobar.foobar_id)
    async with db.get_session(reuse_session) as session:
        response = await session.execute(query)
        inserted_id = response.scalar_one_or_none()
    return inserted_id
