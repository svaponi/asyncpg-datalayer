import contextlib
import logging
import typing
from typing import AsyncIterator

import asyncpg
import sqlalchemy
import sqlalchemy.exc
from asyncpg_datalayer.errors import (
    TooManyConnectionsException,
    PoolOverflowException,
    ConstraintViolationException,
)
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
    AsyncConnection,
)

_REQUIRED_PREFIX = "postgresql+asyncpg://"
_AUTOCORRECT_PREFIX = "postgresql://"


def patch_sqlalchemy_logger():
    logger = logging.getLogger("sqlalchemy.engine.Engine")
    for h in logger.handlers:
        logger.removeHandler(h)


def sanitize_postgres_url(postgres_url: str):
    if postgres_url.startswith(_AUTOCORRECT_PREFIX):
        postgres_url = _REQUIRED_PREFIX + postgres_url.removeprefix(_AUTOCORRECT_PREFIX)
    if not postgres_url.startswith(_REQUIRED_PREFIX):
        raise RuntimeError(
            f"postgres_url should start with {_REQUIRED_PREFIX} ot {_AUTOCORRECT_PREFIX}"
        )
    return postgres_url


def get_asyncpg_cause(err: Exception):
    if isinstance(err, asyncpg.PostgresError):
        return err
    if hasattr(err, "__cause__") and isinstance(err.__cause__, Exception):
        return get_asyncpg_cause(err.__cause__)
    if hasattr(err, "orig") and isinstance(err.orig, Exception):
        return get_asyncpg_cause(err.orig)
    return err


class DB:

    def __init__(
        self,
        postgres_url: str,
        # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.echo
        echo: bool | None = None,
        # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.echo_pool
        echo_pool: bool | None = None,
        # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_size
        pool_size: int | None = None,
        # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_timeout
        pool_timeout: int | None = None,
        # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.max_overflow
        max_overflow: int | None = None,
        **kwargs,
    ):
        """
        If used within the application lifetime, the session can be initiated at startup and closed at shutdown, with
        the setup() and close() methods respectively.
        """

        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.postgres_url = postgres_url
        self.echo = echo
        self.echo_pool = echo_pool
        self.pool_size = pool_size
        self.pool_timeout = pool_timeout
        self.max_overflow = max_overflow

        # remove any None values from kwargs, it won't be accepted by create_async_engine
        engine_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        self.logger.info(f"create_async_engine kwargs: {engine_kwargs}")
        self.engine = create_async_engine(
            sanitize_postgres_url(postgres_url),
            future=True,
            pool_pre_ping=True,  # prevents connection is closed error, see https://sqlalche.me/e/20/rvf5
            echo=self.echo,
            echo_pool=self.echo_pool,
            pool_size=self.pool_size,
            pool_timeout=self.pool_timeout,
            max_overflow=self.max_overflow,
            **engine_kwargs,
        )

        # disable expire_on_commit to prevent DetachedInstanceError, see https://stackoverflow.com/a/58531938/6740561
        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)
        patch_sqlalchemy_logger()

    async def disconnect(self) -> None:
        if self.engine:
            await self.engine.dispose()

    def _map_error(self, err: Exception) -> Exception:
        # https://docs.sqlalchemy.org/en/20/core/exceptions.html#sqlalchemy.exc.TimeoutError
        if isinstance(err, sqlalchemy.exc.TimeoutError):
            err = PoolOverflowException(err)
        elif isinstance(err, asyncpg.TooManyConnectionsError):
            err = TooManyConnectionsException(err)
        elif isinstance(err, asyncpg.InternalServerError):
            if "remaining connection slots are reserved" in str(err):
                err = TooManyConnectionsException(err)

        if isinstance(err, sqlalchemy.exc.IntegrityError):
            # extract the original asyncpg cause to get more details
            asyncpg_err: asyncpg.PostgresError = get_asyncpg_cause(err)
            if isinstance(asyncpg_err, asyncpg.IntegrityConstraintViolationError):
                err = ConstraintViolationException(asyncpg_err)

        # log the error before returning!
        self.logger.exception(err)
        return err

    @contextlib.asynccontextmanager
    async def get_session(
        self,
        reuse_session: AsyncSession = None,
        readonly: bool | None = None,
    ) -> AsyncIterator[AsyncSession]:
        if reuse_session:
            assert isinstance(
                reuse_session, AsyncSession
            ), "reuse_session not of type AsyncSession"
            # no need to handle error here, as it will be handled by the caller
            yield reuse_session
        else:
            try:
                async with self.async_session() as new_session:
                    yield new_session
                    if not readonly:
                        await new_session.commit()
            except Exception as err:
                await new_session.rollback()
                raise self._map_error(err)
            finally:
                await new_session.close()

    @contextlib.asynccontextmanager
    async def connection(self) -> AsyncIterator[AsyncConnection]:
        async with self.engine.begin() as connection:
            try:
                yield connection
            except Exception as err:
                await connection.rollback()
                raise self._map_error(err)

    async def exec_sql(
        self,
        sql: str,
        **kwargs,
    ) -> list[dict[str, typing.Any]]:
        async with self.connection() as conn:
            query = sqlalchemy.text(sql)
            for k, v in kwargs.items():
                if isinstance(v, set):
                    v = list(v)
                bind = sqlalchemy.bindparam(
                    key=k, value=v, expanding=isinstance(v, list)
                )
                query = query.bindparams(bind)
            response = await conn.execute(query)
            results = response.mappings().all()
        return [dict(**r) for r in results]

    async def exec_dml(
        self,
        dml: str,
        **kwargs,
    ) -> int:
        async with self.connection() as conn:
            response = await conn.execute(sqlalchemy.text(dml).params(**kwargs))
            result: int = response.rowcount
        return result

    async def exec_ddl(
        self,
        ddl: str,
    ) -> int:
        async with self.connection() as conn:
            response = await conn.execute(sqlalchemy.DDL(ddl))
            result: int = response.rowcount
        return result
