import os
import typing
from itertools import groupby
from operator import itemgetter

import asyncpg
import pydantic


class ColumnMetadata(pydantic.BaseModel):
    ordinal_position: int
    column_name: str
    data_type: str
    udt_name: str
    is_nullable: bool
    is_primary_key: bool
    column_default: str | None


class TableMetadata(pydantic.BaseModel):
    table_name: str
    columns_meta: typing.List[ColumnMetadata]
    pks: typing.List[ColumnMetadata] = pydantic.Field(default_factory=list)

    def model_post_init(self, __context):
        self.pks = [c for c in self.columns_meta if c.is_primary_key]

    @property
    def column_names(self) -> typing.List[str]:
        return [c.column_name for c in self.columns_meta]

    @property
    def pk_names(self) -> list[str]:
        return [c.column_name for c in self.pks]


_tables: list[TableMetadata] | None = None


async def load_metadata(
    connection: asyncpg.Connection,
) -> list[TableMetadata]:
    global _tables
    if _tables is None:
        filename = "metadata.sql"
        path = os.path.join(os.path.dirname(__file__), filename)
        with open(path, "r") as f:
            sql_for_table_metadata = f.read()
        assert sql_for_table_metadata, f"{filename} file is empty"

        results = await connection.fetch(sql_for_table_metadata)
        records = [dict(**row) for row in results]
        if not records:
            raise RuntimeError(f"no metadata found, there are no tables in the DB")

        _tables = []

        records.sort(key=itemgetter("table_name"))
        records_by_table_name = groupby(records, key=itemgetter("table_name"))
        for table_name, group in records_by_table_name:
            table_records = list(group)
            [row.pop("table_name", None) for row in table_records]
            _tables.append(
                TableMetadata(
                    table_name=table_name,
                    columns_meta=[ColumnMetadata(**dict(row)) for row in table_records],
                )
            )
    return _tables
