import os

import pytest

import asyncpg_datalayer
from asyncpg_datalayer.codegen.main import generate_code
from asyncpg_datalayer.migrationtool.main import apply_migrations


@pytest.mark.asyncio
async def test_generate_code(postgres_url):
    asyncpg_datalayer_dir = os.path.dirname(asyncpg_datalayer.__file__)
    src_dir = os.path.dirname(asyncpg_datalayer_dir)
    migrations_dir = os.path.join(src_dir, "_migrations")
    await apply_migrations(postgres_url, migrations_dir)
    codegen_dir = os.path.join(src_dir, "_generated")
    await generate_code(postgres_url, codegen_dir)
