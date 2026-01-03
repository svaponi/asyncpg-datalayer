import asyncio
import contextlib
import datetime
import logging
import typing
import uuid

import pydantic
import sqlalchemy
import sqlalchemy.ext
import sqlalchemy.ext.asyncio
import sqlalchemy.orm

from asyncpg_datalayer.criteria import Criteria
from asyncpg_datalayer.db import DB
from asyncpg_datalayer.pagination import with_pagination
from asyncpg_datalayer.pagination_and_sorting import parse_sort_by
from asyncpg_datalayer.scrolling import build_cursor, with_scrolling
from asyncpg_datalayer.types import Col, Obj, Filters

_LAST_MODIFIED_AT = "last_modified_at"
_LAST_MODIFIED_BY = "last_modified_by"
_LAST_UPDATED_AT = "last_updated_at"
_LAST_UPDATED_BY = "last_updated_by"
_CREATED_AT = "created_at"
_CREATED_BY = "created_by"
AUDIT_FIELDS: set = {
    _LAST_MODIFIED_AT,
    _LAST_MODIFIED_BY,
    _LAST_UPDATED_AT,
    _LAST_UPDATED_BY,
    _CREATED_AT,
    _CREATED_BY,
}

Record = typing.TypeVar("Record", bound=sqlalchemy.orm.DeclarativeBase)


class BaseRepository(typing.Generic[Record]):
    def __init__(
        self,
        db: DB,
        record_cls: typing.Type[Record],
    ) -> None:
        super().__init__()
        self.logger = logging.getLogger(f"{self.__module__}.{type(self).__name__}")
        self.db = db
        self.record_cls = record_cls
        self.table = sqlalchemy.inspect(record_cls.__table__)
        self.column_names = {c.name for c in self.table.columns}
        self.column_by_name = {c.name: c for c in self.table.columns}
        self.primary_keys: list[Col] = [
            getattr(record_cls, c.name)
            for c in record_cls.__table__.primary_key.columns.values()
        ]

    @property
    def primary_key(self) -> Col:
        """
        This method ensures that the table has exactly one primary key column.
        This is useful because it throws exception if we are invoking a method that was designed for single-column
        primary key tables on multiple-columns primary key preventing misbehavior.
        """
        assert len(self.primary_keys) == 1, "Only single primary key is supported"
        return self.primary_keys[0]

    def _validate_obj(self, record: dict) -> dict:
        for col_name, col_val in record.items():
            if col_name not in self.column_by_name:
                table = self.table.name
                raise ValueError(f"{col_name=} does not exist in {table=}")
            column = self.column_by_name[col_name]
            if col_val is None and column.nullable:
                continue
            if col_val is None and not column.nullable:
                raise ValueError(f"found {col_val=} for non-nullable {col_name=}")
            if not isinstance(col_val, column.type.python_type):
                expected = column.type.python_type
                found = type(col_val)
                raise ValueError(
                    f"invalid type for {col_name=}: {expected=}, {found=} ({col_val=})"
                )
        return record

    def _get_col(self, col_name: str) -> Col:
        assert hasattr(self.record_cls, col_name), f"Column {col_name} not found"
        return getattr(self.record_cls, col_name)

    def _with_filters(
        self,
        query: sqlalchemy.Select | sqlalchemy.Update | sqlalchemy.Delete,
        filters: Filters | None,
    ):
        if filters:
            if isinstance(filters, pydantic.BaseModel):
                filters = filters.model_dump()
            if isinstance(filters, dict):
                filters = list(filters.items())
            expr = Criteria(self._get_col).build_where_expr(filters)
            query = query.where(expr)
        return query

    @contextlib.asynccontextmanager
    async def get_session(
        self,
        readonly: bool | None = None,
    ) -> typing.AsyncIterator[sqlalchemy.ext.asyncio.AsyncSession]:
        async with self.db.get_session(readonly=readonly) as session:
            yield session

    async def count(
        self,
        filters: Filters | None = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> int:
        count_query = sqlalchemy.select(sqlalchemy.func.count()).select_from(
            self.record_cls
        )
        count_query = self._with_filters(count_query, filters)
        async with self.db.get_session(reuse_session, readonly=True) as session:
            count_response = await session.execute(count_query)
            count = count_response.scalar()
        return count

    async def scroll(
        self,
        size: int,
        cursor: str | None = None,
        sort_by: str | None = None,
        filters: Filters | None = None,
        skip_count: bool = False,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> tuple[list[Record], int, str]:
        query = sqlalchemy.select(self.record_cls)
        query = self._with_filters(query, filters)
        if skip_count:
            count_query = None
        else:
            count_query = sqlalchemy.select(sqlalchemy.func.count()).select_from(
                self.record_cls
            )
            count_query = self._with_filters(count_query, filters)

        sort_asc = True
        sort_col = self.primary_keys[0]
        order_by_cols = self.primary_keys[1:]
        if sort_by:
            sort_field, sort_asc = parse_sort_by(sort_by)
            sort_col = self._get_col(sort_field)
            order_by_cols = self.primary_keys

        query = with_scrolling(
            query=query,
            cursor=cursor,
            size=size or 10,
            sort_asc=sort_asc,
            sort_col=sort_col,
            order_by_cols=order_by_cols,
        )

        async def _get_results():
            async with self.db.get_session(reuse_session, readonly=True) as session1:
                return (await session1.execute(query)).scalars().all()

        async def _get_count():
            async with self.db.get_session(reuse_session, readonly=True) as session2:
                return (await session2.execute(count_query)).scalar()

        if count_query is not None:
            results, count = await asyncio.gather(_get_results(), _get_count())
        else:
            results = await _get_results()
            count = -1

        last_cursor = None
        if results:
            last_record = results[-1]
            last_cursor = build_cursor(
                last_record,
                sort_col=sort_col,
                order_by_cols=order_by_cols,
            )
        return results, count, last_cursor

    async def get_page(
        self,
        page: int | None = None,
        size: int | None = None,
        sort_by: str | None = None,
        filters: Filters | None = None,
        skip_count: bool = False,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> tuple[list[Record], int]:
        query = sqlalchemy.select(self.record_cls)
        query = self._with_filters(query, filters)
        if skip_count:
            count_query = None
        else:
            count_query = sqlalchemy.select(sqlalchemy.func.count()).select_from(
                self.record_cls
            )
            count_query = self._with_filters(count_query, filters)

        sort_asc = True
        sort_col = self.primary_keys[0]
        order_by_cols = self.primary_keys[1:]
        if sort_by:
            sort_field, sort_asc = parse_sort_by(sort_by)
            sort_col = self._get_col(sort_field)
            order_by_cols = self.primary_keys

        query = with_pagination(
            query=query,
            page=page or 1,
            size=size or 10,
            sort_asc=sort_asc,
            sort_col=sort_col,
            order_by_cols=order_by_cols,
        )

        async def _get_results():
            async with self.db.get_session(reuse_session, readonly=True) as session1:
                return (await session1.execute(query)).scalars().all()

        async def _get_count():
            async with self.db.get_session(reuse_session, readonly=True) as session2:
                return (await session2.execute(count_query)).scalar()

        if count_query is not None:
            results, count = await asyncio.gather(_get_results(), _get_count())
        else:
            results = await _get_results()
            count = -1
        return results, count

    async def get_all(
        self,
        filters: Filters | None = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> list[Record]:
        results, _ = await self.get_page(
            None,
            reuse_session=reuse_session,
            skip_count=True,
            filters=filters,
        )
        return results

    async def get_one(
        self,
        filters: Filters | None = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> Record | None:
        query = sqlalchemy.select(self.record_cls)
        query = self._with_filters(query, filters)
        async with self.db.get_session(reuse_session, readonly=True) as session:
            response = await session.execute(query)
            result = response.scalar_one_or_none()
        return result

    async def get_or_none_by_id_multikey(
        self,
        entity_ids: tuple[uuid.UUID],
        filters: Filters | None = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> Record | None:
        query = sqlalchemy.select(self.record_cls)
        for pk, entity_id in zip(self.primary_keys, entity_ids):
            query = query.where(pk.__eq__(entity_id))
        query = self._with_filters(query, filters)
        async with self.db.get_session(reuse_session, readonly=True) as session:
            response = await session.execute(query)
            result = response.scalar_one_or_none()
        return result

    async def get_or_none_by_id(
        self,
        entity_id: uuid.UUID,
        filters: Filters | None = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> Record | None:
        query = sqlalchemy.select(self.record_cls).where(
            self.primary_key.__eq__(entity_id)
        )
        query = self._with_filters(query, filters)
        async with self.db.get_session(reuse_session, readonly=True) as session:
            response = await session.execute(query)
            result = response.scalar_one_or_none()
        return result

    async def get_by_ids(
        self,
        entity_ids: set[uuid.UUID],
        filters: Filters | None = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> dict[uuid.UUID, Record]:
        if not entity_ids:
            return {}
        query = sqlalchemy.select(self.record_cls).where(
            self.primary_key.in_(entity_ids)
        )
        query = self._with_filters(query, filters)
        async with self.db.get_session(reuse_session, readonly=True) as session:
            response = await session.execute(query)
            results = response.scalars().all()
        return {getattr(r, self.primary_key.name): r for r in results}

    async def delete_by_id(
        self,
        entity_id: uuid.UUID,
        filters: Filters | None = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> int:
        query = sqlalchemy.delete(self.record_cls).where(
            self.primary_key.__eq__(entity_id)
        )
        query = self._with_filters(query, filters)
        async with self.db.get_session(reuse_session) as session:
            response = await session.execute(query)
            result = response.rowcount
        self.logger.info(f"Deleted {self.record_cls.__name__} with id {entity_id}")
        return result

    async def delete_by_ids(
        self,
        entity_ids: set[uuid.UUID],
        filters: Filters | None = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> int:
        if not entity_ids:
            return 0
        query = sqlalchemy.delete(self.record_cls).where(
            self.primary_key.in_(entity_ids)
        )
        query = self._with_filters(query, filters)
        async with self.db.get_session(reuse_session) as session:
            response = await session.execute(query)
            rowcount = response.rowcount
        self.logger.info(
            f"Deleted {rowcount} {self.record_cls.__name__} with ids {entity_ids}"
        )
        return rowcount

    async def delete_many(
        self,
        filters: Filters | None = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> int:
        assert filters, "delete_many requires at least one where condition"
        query = sqlalchemy.delete(self.record_cls)
        query = self._with_filters(query, filters)
        async with self.db.get_session(reuse_session) as session:
            response = await session.execute(query)
            rowcount = response.rowcount
        self.logger.info(f"Deleted {rowcount} {self.record_cls.__name__} by {filters=}")
        return rowcount

    def _dump_update_obj(self, update_obj: Obj):
        if isinstance(update_obj, pydantic.BaseModel):
            update_obj = update_obj.model_dump(exclude_unset=True)
        return update_obj

    def _set_audit_for_update(self, obj: dict, user_id: str = None):
        invalid_fields = AUDIT_FIELDS.intersection(obj.keys())
        if invalid_fields:
            raise ValueError(f"cannot set audit fields: {invalid_fields}")
        if user_id and _LAST_UPDATED_BY in self.column_names:
            obj[_LAST_UPDATED_BY] = user_id
        if _LAST_UPDATED_AT in self.column_names:
            obj[_LAST_UPDATED_AT] = datetime.datetime.now()
        if user_id and _LAST_MODIFIED_BY in self.column_names:
            obj[_LAST_MODIFIED_BY] = user_id
        if _LAST_MODIFIED_AT in self.column_names:
            obj[_LAST_MODIFIED_AT] = datetime.datetime.now()
        return obj

    async def update_by_id(
        self,
        entity_id: uuid.UUID,
        update_obj: Obj,
        filters: Filters | None = None,
        user_id: str | uuid.UUID = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> int:

        obj = self._dump_update_obj(update_obj)
        obj = self._validate_obj(obj)
        obj = self._set_audit_for_update(obj, user_id)

        query = (
            sqlalchemy.update(self.record_cls)
            .where(self.primary_key.__eq__(entity_id))
            .values(**obj)
        )
        query = self._with_filters(query, filters)
        async with self.db.get_session(reuse_session) as session:
            response = await session.execute(query)
            result = response.rowcount
        self.logger.info(f"Updated {self.record_cls.__name__} with id {entity_id}")
        return result

    async def update_many(
        self,
        update_obj: Obj,
        filters: Filters | None = None,
        user_id: str | uuid.UUID = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> int:
        assert filters, "delete_many requires at least one where condition"

        obj = self._dump_update_obj(update_obj)
        obj = self._validate_obj(obj)
        obj = self._set_audit_for_update(obj, user_id)

        query = sqlalchemy.update(self.record_cls).values(**obj)
        query = self._with_filters(query, filters)
        async with self.db.get_session(reuse_session) as session:
            response = await session.execute(query)
            rowcount = response.rowcount
        self.logger.info(f"Updated {rowcount} {self.record_cls.__name__} by {filters=}")
        return rowcount

    def _dump_insert_obj(self, insert_obj: Obj):
        if isinstance(insert_obj, pydantic.BaseModel):
            insert_obj = insert_obj.model_dump()
        return insert_obj

    def _set_audit_for_insert(self, obj: dict, user_id: str = None):
        invalid_fields = AUDIT_FIELDS.intersection(obj.keys())
        if invalid_fields:
            raise ValueError(f"cannot set audit fields: {invalid_fields}")
        if user_id and _CREATED_BY in self.column_names:
            obj[_CREATED_BY] = user_id
        if _CREATED_AT in self.column_names:
            obj[_CREATED_AT] = datetime.datetime.now()
        if user_id and _LAST_MODIFIED_BY in self.column_names:
            obj[_LAST_MODIFIED_BY] = user_id
        if _LAST_MODIFIED_AT in self.column_names:
            obj[_LAST_MODIFIED_AT] = datetime.datetime.now()
        return obj

    async def insert(
        self,
        insert_obj: Obj,
        user_id: str | uuid.UUID = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> uuid.UUID:

        obj = self._dump_insert_obj(insert_obj)
        obj = self._validate_obj(obj)
        obj = self._set_audit_for_insert(obj, user_id)

        query = (
            sqlalchemy.insert(self.record_cls).values(**obj).returning(self.primary_key)
        )
        async with self.db.get_session(reuse_session) as session:
            response = await session.execute(query)
            inserted_id = response.scalar_one_or_none()
        self.logger.info(f"Created {self.record_cls.__name__} with id {inserted_id}")
        return inserted_id

    async def insert_many(
        self,
        insert_objs: list[Obj],
        user_id: str | uuid.UUID = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> list[uuid.UUID]:
        if not insert_objs:
            return []

        objs = [self._dump_insert_obj(insert_obj) for insert_obj in insert_objs]
        objs = [self._validate_obj(obj) for obj in objs]
        objs = [self._set_audit_for_insert(obj, user_id) for obj in objs]

        query = (
            sqlalchemy.insert(self.record_cls).values(objs).returning(self.primary_key)
        )
        async with self.db.get_session(reuse_session) as session:
            response = await session.execute(query)
            inserted_ids = response.scalars().all()
        self.logger.info(
            f"Created {len(inserted_ids)} {self.record_cls.__name__} with ids {inserted_ids}"
        )
        return inserted_ids

    async def insert_multikey(
        self,
        insert_obj: Obj,
        user_id: str | uuid.UUID = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> tuple[uuid.UUID]:

        obj = self._dump_insert_obj(insert_obj)
        obj = self._validate_obj(obj)
        obj = self._set_audit_for_insert(obj, user_id)

        query = (
            sqlalchemy.insert(self.record_cls)
            .values(**obj)
            .returning(*self.primary_keys)
        )
        async with self.db.get_session(reuse_session) as session:
            response = await session.execute(query)
            inserted_id_multikey = response.fetchone()
        self.logger.info(
            f"Created {self.record_cls.__name__} with id {inserted_id_multikey}"
        )
        return inserted_id_multikey

    async def insert_many_multikey(
        self,
        insert_objs: list[Obj],
        user_id: str | uuid.UUID = None,
        reuse_session: sqlalchemy.ext.asyncio.AsyncSession = None,
    ) -> list[tuple[uuid.UUID]]:
        if not insert_objs:
            return []

        objs = [self._dump_insert_obj(insert_obj) for insert_obj in insert_objs]
        objs = [self._validate_obj(obj) for obj in objs]
        objs = [self._set_audit_for_insert(obj, user_id) for obj in objs]

        query = (
            sqlalchemy.insert(self.record_cls)
            .values(objs)
            .returning(*self.primary_keys)
        )
        async with self.db.get_session(reuse_session) as session:
            response = await session.execute(query)
            inserted_ids_multikey = response.fetchall()
        self.logger.info(
            f"Created {len(inserted_ids_multikey)} {self.record_cls.__name__} with ids {inserted_ids_multikey}"
        )
        return inserted_ids_multikey
