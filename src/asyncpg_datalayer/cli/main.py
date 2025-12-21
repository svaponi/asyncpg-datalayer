import argparse
import asyncio
import os
import sys

import dotenv

from asyncpg_datalayer.migrationtool.main import apply_migrations


def _migrations(postgres_url: str, migrations_dir: str) -> None:
    if not postgres_url:
        raise RuntimeError(
            "Postgres URL must be provided via --postgres-url or POSTGRES_URL env var"
        )

    if not migrations_dir:
        raise RuntimeError(
            "Source directory for migrationtool (.sql scripts) must be provided via --migrationtool-dir or MIGRATIONS_DIR env var"
        )

    if not os.path.isdir(migrations_dir):
        raise RuntimeError(f"Migrations directory not found: {migrations_dir}")

    asyncio.run(apply_migrations(postgres_url, migrations_dir))


def _codegen(postgres_url: str, codegen_dir: str) -> None:
    if not postgres_url:
        raise RuntimeError(
            "Postgres URL must be provided via --postgres-url or POSTGRES_URL env var"
        )
    if not codegen_dir:
        raise RuntimeError(
            "Destination directory for code generation must be provided via --codegen-dir or CODEGEN_DIR env var"
        )

    try:
        from asyncpg_datalayer.codegen.main import generate_code

        asyncio.run(generate_code(postgres_url, codegen_dir))
    except ImportError as e:
        print(e, file=sys.stderr)
        raise RuntimeError(
            "Some dependencies are missing, please make sure 'asyncpg-datalayer[codegen]' is installed"
        )


def main():
    dotenv_filename = os.environ.get("DOTENV")
    if dotenv_filename:
        dotenv_path = dotenv.find_dotenv(dotenv_filename, raise_error_if_not_found=True)
        dotenv.load_dotenv(dotenv_path=dotenv_path)
    else:
        dotenv.load_dotenv()

    parser = argparse.ArgumentParser(description="Generate code for the data layer")
    subparsers = parser.add_subparsers(dest="action", required=True)

    migrations_parser = subparsers.add_parser("migrate", help="Apply migrations")
    migrations_parser.add_argument(
        "--postgres-url",
        default=os.environ.get("POSTGRES_URL"),
        help="Postgres connection URL (or set POSTGRES_URL env variable)",
    )
    migrations_parser.add_argument(
        "--migrations-dir",
        default=os.environ.get("MIGRATIONS_DIR"),
        help="Migrations directory (or set MIGRATIONS_DIR env variable)",
    )

    codegen_parser = subparsers.add_parser("codegen", help="Generate code from DB")
    codegen_parser.add_argument(
        "--postgres-url",
        default=os.environ.get("POSTGRES_URL"),
        help="Postgres connection URL (or set POSTGRES_URL env variable)",
    )
    codegen_parser.add_argument(
        "--codegen-dir",
        default=os.environ.get("CODEGEN_DIR"),
        help="Directory to output generated code (or set CODEGEN_DIR env variable)",
    )

    args = parser.parse_args()

    try:

        if args.action == "migrate":
            postgres_url = args.postgres_url
            migrations_dir = args.migrations_dir
            _migrations(postgres_url, migrations_dir)

        elif args.action == "codegen":
            postgres_url = args.postgres_url
            codegen_dir = args.codegen_dir
            _codegen(postgres_url, codegen_dir)

        else:
            raise RuntimeError(f"Unknown action: {args.action}")

    except RuntimeError as e:
        red = "\033[91m"
        reset = "\033[0m"
        print(f"{red}{e}{reset}", file=sys.stderr)
        print(file=sys.stderr)
        parser.print_help()
        exit(1)


if __name__ == "__main__":
    main()
