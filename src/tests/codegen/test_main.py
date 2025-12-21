import os
import shutil

import pytest

import asyncpg_datalayer
from asyncpg_datalayer.codegen.main import generate_code
from asyncpg_datalayer.migrationtool.main import apply_migrations


@pytest.mark.asyncio
async def test_generate_code(postgres_url):
    asyncpg_datalayer_dir = os.path.dirname(asyncpg_datalayer.__file__)
    src_dir = os.path.dirname(asyncpg_datalayer_dir)
    migrations_dir = os.path.join(src_dir, "_migrations")
    codegen_dir = os.path.join(src_dir, "_generated")

    # first apply migrations to ensure the database schema is up to date
    await apply_migrations(postgres_url, migrations_dir)

    # clean up any existing generated code
    shutil.rmtree(codegen_dir, ignore_errors=True)
    await generate_code(postgres_url, codegen_dir)

    # now add some custom code to a generated file and ensure it is preserved
    repo_path = os.path.join(codegen_dir, "org_repository.py")
    with open(repo_path, "a") as f:
        f.write("# I am here to stay!\n")
    await generate_code(postgres_url, codegen_dir)
    with open(repo_path) as f:
        repo_code = f.read()
    assert "# I am here to stay!" in repo_code
