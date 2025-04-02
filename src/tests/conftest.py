import pytest
from testcontainers.postgres import PostgresContainer

from asyncpg_datalayer.db import DB


@pytest.fixture
def postgres_url():
    with PostgresContainer("postgres:16-alpine", driver=None) as postgres:
        yield postgres.get_connection_url()


@pytest.fixture
def db(postgres_url):
    yield DB(postgres_url, echo=True)
