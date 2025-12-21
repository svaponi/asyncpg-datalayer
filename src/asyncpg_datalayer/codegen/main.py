# This script generates pydantic classes from an existing Postgres DB.
# Such classes are meant to be used as generic types (Record, RecordUpdate and RecordInsert) for BaseRepository.

import os
import pathlib
import shutil
import tempfile

import asyncpg

from asyncpg_datalayer.base_repository import AUDIT_FIELDS
from asyncpg_datalayer.codegen.metadata import TableMetadata, load_metadata


async def generate_code(postgres_url: str, codegen_dir: str):
    await _Codegen(postgres_url, codegen_dir).generate()


class _Codegen:
    _POSTGRES_TYPE_TO_PYTHON_TYPE = {
        "uuid": "uuid.UUID",
        "json": "dict",
        "integer": "int",
        "smallint": "int",
        "bigint": "int",
        "serial": "int",
        "bigserial": "int",
        "decimal": "float",
        "numeric": "float",
        "real": "float",
        "double precision": "float",
        "boolean": "bool",
        "text": "str",
        "varchar": "str",
        "character varying": "str",
        "character": "str",
        "date": "datetime.date",
        "timestamp": "datetime.datetime",
        "timestamp without time zone": "datetime.datetime",
        "timestamp with time zone": "datetime.datetime",
        "time": "datetime.time",
        "time without time zone": "datetime.time",
        "time with time zone": "datetime.time",
    }

    _POSTGRES_TYPE_TO_SQLALCHEMY_TYPE = {
        "uuid": "sqlalchemy.Uuid",
        "json": "sqlalchemy.JSON",
        "integer": "sqlalchemy.Integer",
        "smallint": "sqlalchemy.SmallInt",
        "bigint": "sqlalchemy.BigInt",
        "serial": "sqlalchemy.Integer",
        "bigserial": "sqlalchemy.BigInt",
        "decimal": "sqlalchemy.Numeric",
        "numeric": "sqlalchemy.Numeric",
        "real": "sqlalchemy.Float",
        "double precision": "sqlalchemy.Double",
        "boolean": "sqlalchemy.Boolean",
        "text": "sqlalchemy.String",
        "varchar": "sqlalchemy.String",
        "character varying": "sqlalchemy.String",
        "character": "sqlalchemy.String",
        "date": "sqlalchemy.Date",
        "timestamp": "sqlalchemy.DateTime",
        "timestamp without time zone": "sqlalchemy.DateTime",
        "timestamp with time zone": "sqlalchemy.DateTime",
        "time": "sqlalchemy.Time",
        "time without time zone": "sqlalchemy.Time",
        "time with time zone": "sqlalchemy.Time",
    }

    try:
        import fastapi

        _HAS_FASTAPI = True
    except ImportError:
        _HAS_FASTAPI = False

    _CUSTOM_METHODS_DEL = "### custom methods go below ###"

    def __init__(self, postgres_url: str, codegen_dir: str):
        super().__init__()
        self.postgres_url = postgres_url
        self.codegen_dir = codegen_dir
        self.codegen_dir_bak = None
        self._excluded_tables = ["_migrations"]
        self._env = None

    def _jinja2(self):
        if not self._env:
            from jinja2 import Environment, FileSystemLoader

            # Create a Jinja2 environment and load templates from the current directory
            templates_dir = os.path.join(os.path.dirname(__file__), "templates")
            self._env = Environment(loader=FileSystemLoader(templates_dir))
        return self._env

    def _map_column_default_to_python_value(self, column_default):
        if column_default is None:
            return "None"
        if column_default == "true":
            return "True"
        if column_default == "false":
            return "False"
        if column_default == "NULL":
            return "None"
        if column_default.startswith("uuid_generate_v4"):
            return "uuid.uuid4()"
        if column_default.startswith("now()"):
            return "datetime.datetime.now()"
        if column_default.startswith("ARRAY[]"):
            return "pydantic.Field(default_factory=list)"
        raise RuntimeError(f"unsupported {column_default=}")

    def _map_postgres_type_to_python_type(self, postgres_type, udt_name):
        if postgres_type == "ARRAY":
            array_item_type = udt_name.removeprefix("_")
            python_type = (
                f"list[{self._map_postgres_type_to_python_type(array_item_type, None)}]"
            )
        else:
            python_type = self._POSTGRES_TYPE_TO_PYTHON_TYPE.get(postgres_type)
        assert python_type, f"unsupported {postgres_type=}"
        return python_type

    def _map_postgres_type_to_sqlalchemy_type(self, postgres_type, udt_name):
        # See https://docs.sqlalchemy.org/en/20/core/type_basics.html#generic-camelcase-types
        if postgres_type == "ARRAY":
            array_item_type = udt_name.removeprefix("_")
            sqlalchemy_type = f"sqlalchemy.ARRAY({self._map_postgres_type_to_sqlalchemy_type(array_item_type, None)})"
        else:
            sqlalchemy_type = self._POSTGRES_TYPE_TO_SQLALCHEMY_TYPE.get(postgres_type)
        assert sqlalchemy_type, f"unsupported {sqlalchemy_type=}"
        return sqlalchemy_type

    def _generate_repository_code(self, table: TableMetadata):
        template = self._jinja2().get_template("repository.tmpl")

        table_name = table.table_name
        table_pk_names = table.pk_names
        entity_name = "".join(s.capitalize() for s in table_name.split("_"))

        table_field_defs = []
        table_constraint_defs = []
        record_insert_field_defs = []
        record_update_field_defs = []
        for column in table.columns_meta:
            column_name = column.column_name
            python_type = self._map_postgres_type_to_python_type(
                column.data_type, column.udt_name
            )
            if column.is_nullable:
                python_type = f"{python_type} | None"
            sa_type = self._map_postgres_type_to_sqlalchemy_type(
                column.data_type, column.udt_name
            )
            if column.is_primary_key:
                sa_type = f"{sa_type}, primary_key=True"
            if column.column_default:
                sa_type = f'{sa_type}, server_default=sqlalchemy.text("{column.column_default}")'
            table_field_defs.append(
                f"{column_name}: sqlalchemy.orm.Mapped[{python_type}] = sqlalchemy.orm.mapped_column({sa_type})"
            )
            if column_name == "org_id" and table_name != "org":
                table_constraint_defs.append(
                    f'sqlalchemy.ForeignKeyConstraint(["org_id"], ["org.org_id"], name="{table_name}_org_id_fkey")'
                )

            if column_name in AUDIT_FIELDS:
                continue
            python_type = self._map_postgres_type_to_python_type(
                column.data_type, column.udt_name
            )
            if column.is_primary_key:
                pk_field_def = f"{column_name}: {python_type}"
                if column.data_type == "uuid":
                    pk_field_def = f"{column_name}: {python_type} = pydantic.Field(default_factory=uuid.uuid4)"
                record_insert_field_defs.append(pk_field_def)
                continue
            if column.is_nullable:
                column_default = self._map_column_default_to_python_value(
                    column.column_default
                )
                record_insert_field_defs.append(
                    f"{column_name}: {python_type} | None = {column_default}"
                )
            elif column.column_default:
                column_default = self._map_column_default_to_python_value(
                    column.column_default
                )
                record_insert_field_defs.append(
                    f"{column_name}: {python_type} = {column_default}"
                )
            else:
                record_insert_field_defs.append(f"{column_name}: {python_type}")
            record_update_field_defs.append(
                f"{column_name}: {python_type} | None = None"
            )

        context = {
            "entity_name": entity_name,
            "table_name": table_name,
            "table_pk_names": table_pk_names,
            "table_field_defs": table_field_defs,
            "table_constraint_defs": table_constraint_defs,
            "record_insert_field_defs": record_insert_field_defs,
            "record_update_field_defs": record_update_field_defs,
            "has_fastapi": self._HAS_FASTAPI,
            "custom_methods_delimiter": self._CUSTOM_METHODS_DEL,
        }

        rendered = template.render(context)
        return rendered

    def _generate_repository(self, table: TableMetadata):
        code = self._generate_repository_code(table)
        filepath = os.path.join(self.codegen_dir, f"{table.table_name}_repository.py")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Preserve custom methods from backup if any
        if os.path.exists(self.codegen_dir_bak):
            filepath_bak = os.path.join(
                self.codegen_dir_bak, f"{table.table_name}_repository.py"
            )
            if os.path.exists(filepath_bak):
                with open(filepath_bak, "r") as f:
                    existing_code = f.read()
                custom_methods = existing_code.split(self._CUSTOM_METHODS_DEL, 1)[1]
                if custom_methods:
                    if custom_methods.strip():
                        custom_methods = f"\n\n{custom_methods}"
                        code += custom_methods

        with open(filepath, "w") as f:
            f.write(code)
        return filepath

    async def generate(self):
        files = []

        if os.path.exists(self.codegen_dir):
            self.codegen_dir_bak = tempfile.mkdtemp()
            shutil.copytree(self.codegen_dir, self.codegen_dir_bak, dirs_exist_ok=True)
            shutil.rmtree(self.codegen_dir, ignore_errors=True)

        os.makedirs(self.codegen_dir)
        _init = os.path.join(self.codegen_dir, "__init__.py")
        pathlib.Path(_init).touch(exist_ok=True)

        connection = await asyncpg.connect(self.postgres_url)
        try:
            _metadata = await load_metadata(connection)
            for table in _metadata:
                if table.table_name in self._excluded_tables:
                    continue
                files.append(self._generate_repository(table))

        finally:
            await connection.close()

        os.system(f"python -m black {" ".join(filepath for filepath in files)}")
