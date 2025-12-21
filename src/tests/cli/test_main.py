import sys

import pytest

from asyncpg_datalayer.cli.main import main
from tests.testutils.mock_environ import mock_environ


def test_cli_happy_path(monkeypatch, postgres_url, migrations_dir):
    with mock_environ(DOTENV=""):
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "prog",
                "migrate",
                "--postgres-url",
                postgres_url,
                "--migrations-dir",
                migrations_dir,
            ],
        )
        main()


def test_cli_supports_with_env_vars(monkeypatch, postgres_url, migrations_dir):
    with mock_environ(POSTGRES_URL=postgres_url, MIGRATIONS_DIR=migrations_dir):
        monkeypatch.setattr(sys, "argv", ["prog", "migrate"])
        main()


def test_cli_supports_with_dotenv(monkeypatch, postgres_url):
    with mock_environ(DOTENV=".env.test"):
        monkeypatch.setattr(sys, "argv", ["prog", "migrate"])
        main()


def test_cli_missing_postgres(monkeypatch):
    with mock_environ(POSTGRES_URL="", MIGRATIONS_DIR=""):
        monkeypatch.setattr(sys, "argv", ["prog", "migrate"])
        with pytest.raises(SystemExit):
            main()
