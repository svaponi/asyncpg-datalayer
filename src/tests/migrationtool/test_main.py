import pytest

from asyncpg_datalayer.migrationtool.main import apply_migrations


@pytest.mark.asyncio
async def test_apply_migrations(postgres_url, migrations_dir):
    await apply_migrations(postgres_url, migrations_dir)
