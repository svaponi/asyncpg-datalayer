import os
from typing import Mapping

from asyncpg_datalayer.db import DB


def create_db(environ: Mapping = os.environ) -> DB:
    def getenv(key, default: str = None) -> str:
        return environ.get(key, default)

    def getenv_or_fail(key, default: str = None) -> str:
        val = getenv(key, default)
        if val is None:
            raise RuntimeError(f"missing {key}")
        return val

    def getenv_bool(key, default: bool | None = None) -> bool | None:
        val = getenv(key)
        if val is None:
            return default
        val = val.lower()
        if val not in ("true", "false", "1", "0"):
            raise RuntimeError(f"invalid value '{val}' for {key}")
        return val not in ("false", "0")

    return DB(
        getenv_or_fail("POSTGRES_URL"),
        echo=getenv_bool("LOG_SQL"),
        # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_size
        pool_size=getenv("POOL_SIZE"),
        # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_timeout
        pool_timeout=getenv("POOL_TIMEOUT"),
        # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.max_overflow
        max_overflow=getenv("POOL_MAX_OVERFLOW"),
    )
