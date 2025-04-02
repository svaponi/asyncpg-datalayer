import hashlib
import logging
import os
from typing import Dict, List

import asyncpg


class MigrationTool:
    """
    MigrationTool Class

    Manages and applies SQL migrations to a PostgreSQL database using asyncpg.

    Key functionalities:
    - Checks and creates a `migration_history` table if it doesn't exist.
    - Computes SHA-256 hashes of migration files to ensure integrity.
    - Retrieves applied migrations and applies new ones in sorted order.
    - Handles transactions to ensure atomic application of migrations.

    Attributes:
    - connection (asyncpg.Connection): A connection to the PostgreSQL database.

    Methods:
    - `migrate(migrations_dir: str) -> None`: Applies new migrations from the specified directory.
    """

    def __init__(self, connection: asyncpg.Connection) -> None:
        self.connection = connection
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
            "SELECT filename, hash FROM migration_history ORDER BY applied_at"
        )
        return {record["filename"]: record["hash"] for record in records}

    async def _apply_migration(self, filename: str, filepath: str) -> None:
        file_hash = self._calculate_file_hash(filepath)
        with open(filepath, "r") as file:
            sql_content = file.read()
            async with self.connection.transaction():
                await self.connection.execute(sql_content)
                await self.connection.execute(
                    "INSERT INTO migration_history (filename, hash) VALUES ($1, $2)",
                    filename,
                    file_hash,
                )

    async def _check_and_create_history_table(self) -> None:
        result = await self.connection.fetchval(
            """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'migration_history'
        )
        """
        )
        if not result:
            await self.connection.execute(
                """
            CREATE TABLE migration_history (
                id SERIAL PRIMARY KEY,
                filename TEXT NOT NULL,
                hash TEXT NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            )

    def _get_sql_migration_files(self, migrations_dir: str) -> List[str]:
        """Retrieve .sql files sorted by name from the migrations directory."""
        return sorted(
            [file for file in os.listdir(migrations_dir) if file.endswith(".sql")]
        )

    async def migrate(self, migrations_dir: str) -> None:
        await self._check_and_create_history_table()
        applied_migrations = await self._get_applied_migrations()
        migration_files = self._get_sql_migration_files(migrations_dir)

        applied_any = False
        for filename in migration_files:
            filepath = os.path.join(migrations_dir, filename)

            if filename in applied_migrations:
                file_hash = self._calculate_file_hash(filepath)
                if applied_migrations[filename] != file_hash:
                    raise ValueError(
                        f"Hash mismatch for {filename}. Migration file may have been altered."
                    )
                self.logger.debug(f"Skipping migration: {filename} (already applied)")
            else:
                await self._apply_migration(filename, filepath)
                applied_any = True
                self.logger.info(f"Applied migration: {filename}")

        if not applied_any:
            self.logger.debug(f"No migrations applied, database is up-to-date.")
