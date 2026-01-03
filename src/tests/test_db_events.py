import asyncio
import datetime
import uuid

import pytest
import pytest_asyncio

from asyncpg_datalayer.db_events import DBEvents


@pytest_asyncio.fixture(scope="function")
async def events(db):
    events = DBEvents(db, channel="test_events")
    await events.connect()
    yield events
    await events.disconnect()


@pytest.mark.asyncio
async def test_subscribe_and_notify(events):

    async def worker(results):
        async for event in events.listen():
            results.append(event)

    results1 = []
    results2 = []
    results3 = []
    task1 = asyncio.create_task(worker(results1))
    task2 = asyncio.create_task(worker(results2))
    task3 = asyncio.create_task(worker(results3))

    payload = {
        "name": "transaction",
        "user_id": 42,
        "transaction_id": uuid.uuid4(),
        "amount": 123.45,
        "created_at": datetime.datetime.now(),
    }
    await events.notify(payload)
    await events.disconnect()

    await asyncio.gather(task1, task2, task3)

    assert payload in results1
    assert payload in results2
    assert payload in results3
