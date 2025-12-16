import uuid

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from asyncpg_datalayer.db import DB


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16-alpine", driver=None) as postgres:
        yield postgres


@pytest_asyncio.fixture(scope="function")
async def postgres_url(postgres_container: PostgresContainer):
    db_url = postgres_container.get_connection_url()

    async def _exec_stmt(statement: str):
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(statement)
        finally:
            await conn.close()

    db_name = "_" + uuid.uuid4().hex
    await _exec_stmt(f"CREATE DATABASE {db_name};")
    yield db_url.removesuffix(postgres_container.dbname) + db_name
    await _exec_stmt(f"DROP DATABASE IF EXISTS {db_name} WITH(FORCE);")


@pytest.fixture
def db(postgres_url):
    yield DB(postgres_url, echo=True)
