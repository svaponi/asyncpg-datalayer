import asyncio
import contextlib
import datetime
import logging
import typing
from typing import Optional

import asyncpg
import sqlalchemy

from asyncpg_datalayer.db import DB


class DBEvents:
    def __init__(
        self,
        db: DB,
        channel: Optional[str] = None,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.db = db
        self.channel = channel or "events"

        self._connection: Optional[asyncpg.Connection] = None
        self._connection_lock = asyncio.Lock()

        self._reconnect_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

        self._subscribers: list[asyncio.Queue[str | None]] = []

    async def _connect(self) -> None:
        if not self._connection:
            async with self._connection_lock:
                if not self._connection:
                    try:
                        self._connection = await asyncpg.connect(self.db.postgres_url)
                        await self._connection.add_listener(
                            self.channel, self._dispatch
                        )
                        self.logger.info("LISTEN connected on channel %s", self.channel)
                    except Exception:
                        self.logger.exception(
                            "Failed to connect LISTEN on channel %s", self.channel
                        )
                        self._connection = None
                        raise

    async def _disconnect(self) -> None:
        if self._connection:
            async with self._connection_lock:
                if self._connection:
                    await self._connection.close()
                    self._connection = None
                    self.logger.info("LISTEN connection closed")

    def _dispatch(
        self,
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ):
        self._emit(payload)

    def _emit(self, event: str | None):
        for q in self._subscribers:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                self.logger.warning("Subscriber queue full, dropping event")

    async def _reconnect_loop(self):
        retry_delay = 1
        max_delay = 30
        last_attempt = datetime.datetime.now()
        while not self._stop_event.is_set():
            if not self._connection or self._connection.is_closed():
                try:
                    elapsed = datetime.datetime.now() - last_attempt
                    elapsed = elapsed.total_seconds()
                    if elapsed < retry_delay:
                        await asyncio.sleep(retry_delay - elapsed)

                    self.logger.warning("LISTEN connection lost, reconnecting...")
                    last_attempt = datetime.datetime.now()
                    await self._connect()
                    retry_delay = 1  # reset after success
                except Exception:
                    self.logger.error(
                        f"Reconnect failed, retrying in {retry_delay}s..."
                    )
                    retry_delay = min(max_delay, retry_delay * 2)
            else:
                await asyncio.sleep(1)

    async def connect(self) -> None:
        self._stop_event.clear()
        await self._connect()
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def disconnect(self) -> None:
        self._stop_event.set()
        if self._reconnect_task:
            await self._reconnect_task
        self._emit(None)
        await self._disconnect()

    async def notify(self, payload: str) -> None:
        sql = sqlalchemy.text(f"NOTIFY {self.channel}, :payload").bindparams(
            sqlalchemy.bindparam("payload", payload, literal_execute=True)
        )
        async with self.db.connection() as conn:
            await conn.execute(sql)

    @contextlib.contextmanager
    def subscribe(self) -> typing.Generator[asyncio.Queue[str | None], None, None]:
        q = asyncio.Queue(maxsize=1000)
        self._subscribers.append(q)
        yield q
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    async def listen(self) -> typing.AsyncIterator[str]:
        with self.subscribe() as q:
            while True:
                event = await q.get()
                if event is None:
                    break
                yield event
