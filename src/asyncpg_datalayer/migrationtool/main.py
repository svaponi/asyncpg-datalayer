import hashlib
import logging
import os
from typing import Dict, List

import asyncpg


async def apply_migrations(postgres_url: str, migrations_dir: str):
    assert postgres_url, f"postgres_url is undefined"
    assert migrations_dir, f"migrations_dir is undefined"
    assert os.path.isdir(
        migrations_dir
    ), f"migrations_dir is not a directory: {migrations_dir}"
    conn = await asyncpg.connect(postgres_url)
    try:
        await _MigrationTool(conn, migrations_dir).migrate()
    finally:
        await conn.close()


class _MigrationTool:
    """
    MigrationTool Class

    Manages and applies SQL migrationtool to a PostgreSQL database using asyncpg.

    Key functionalities:
    - Checks and creates a migrationtool table if it doesn't exist.
    - Computes SHA-256 hashes of migrationtool files to ensure integrity.
    - Retrieves applied migrationtool and applies new ones in sorted order.
    - Handles transactions to ensure atomic application of migrationtool.

    Attributes:
    - connection (asyncpg.Connection): A connection to the PostgreSQL database.
    - migrations_dir (str): Directory containing migrationtool SQL files.
    - migrations_table (str): Name of the table to track applied migrationtool.

    Methods:
    - `migrate() -> None`: Applies new migrationtool from the specified directory.
    """

    def __init__(
        self,
        connection: asyncpg.Connection,
        migrations_dir: str,
        migrations_table: str | None = None,
    ) -> None:
        assert os.path.isdir(
            migrations_dir
        ), f"migrations_dir is not a directory: {migrations_dir}"
        self.connection = connection
        self.migrations_dir = migrations_dir
        self.migrations_table = migrations_table or "_migrations"
        self.logger = logging.getLogger("migrationtool")

    def _calculate_file_hash(self, filepath: str) -> str:
        """Calculate SHA-256 hash of the given file."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as file:
            while chunk := file.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()

    async def _get_applied_migrations(self) -> Dict[str, str]:
        records = await self.connection.fetch(
            f"SELECT filename, hash FROM {self.migrations_table} ORDER BY applied_at"
        )
        return {record["filename"]: record["hash"] for record in records}

    async def _apply_migration(self, filename: str, filepath: str) -> None:
        file_hash = self._calculate_file_hash(filepath)
        with open(filepath, "r") as file:
            sql_content = file.read()
            async with self.connection.transaction():
                await self.connection.execute(sql_content)
                await self.connection.execute(
                    f"INSERT INTO {self.migrations_table} (filename, hash) VALUES ($1, $2)",
                    filename,
                    file_hash,
                )

    async def _check_and_create_history_table(self) -> None:
        result = await self.connection.fetchval(
            f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{self.migrations_table}'
        )
        """
        )
        if not result:
            await self.connection.execute(
                f"""
            CREATE TABLE {self.migrations_table} (
                id SERIAL PRIMARY KEY,
                filename TEXT NOT NULL,
                hash TEXT NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            )

    def _get_sql_migration_files(self) -> List[str]:
        """Retrieve .sql files sorted by name from the migrationtool directory."""
        return sorted(
            [file for file in os.listdir(self.migrations_dir) if file.endswith(".sql")]
        )

    async def migrate(self) -> None:
        await self._check_and_create_history_table()
        applied_migrations = await self._get_applied_migrations()
        migration_files = self._get_sql_migration_files()

        applied_any = False
        for filename in migration_files:
            filepath = os.path.join(self.migrations_dir, filename)

            if filename in applied_migrations:
                file_hash = self._calculate_file_hash(filepath)
                if applied_migrations[filename] != file_hash:
                    raise ValueError(
                        f"Hash mismatch for {filename}. Migration file may have been altered."
                    )
                self.logger.debug(
                    f"Skipping migrationtool: {filename} (already applied)"
                )
            else:
                await self._apply_migration(filename, filepath)
                applied_any = True
                self.logger.info(f"Applied migrationtool: {filename}")

        if not applied_any:
            self.logger.debug(f"No migrationtool applied, database is up-to-date.")
