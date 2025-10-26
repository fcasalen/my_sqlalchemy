import os
import tempfile
from sqlalchemy import Column, DateTime, Integer, String
from unittest.mock import patch
from sqlalchemy.orm import declarative_base

import pytest

from src.my_sqlalchemy.manager import DatabaseManager
from src.my_sqlalchemy.standard_model import StandardModel
from src.my_sqlalchemy.models_handler import ModelsHandler
from src.my_sqlalchemy.manager import cli

_TestManagerBase = declarative_base()


@pytest.fixture
def temp_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db_url = f"sqlite:///{db_path}"
    yield db_url


class MockModel(StandardModel):
    __tablename__ = "test_table_manager"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    metadata = _TestManagerBase.metadata


@pytest.fixture
def manager(temp_db):
    """Create a DatabaseManager instance with temporary database."""
    manager_instance = DatabaseManager(temp_db)
    manager_instance.models = ModelsHandler([MockModel])
    manager_instance.base = _TestManagerBase
    manager_instance.base.metadata.create_all(manager_instance.engine)
    manager_instance.add(MockModel, [{"name": "Test Name"}])
    yield manager_instance
    manager_instance.engine.dispose()


class TestDatabaseManager:
    def test_init(self, temp_db):
        """Test DatabaseManager initialization."""
        manager = DatabaseManager(temp_db)
        assert manager.database_url == temp_db
        assert hasattr(manager, "base")
        assert hasattr(manager, "engine")
        manager.engine.dispose()

    def test_create_database_success(self, manager: DatabaseManager):
        """Test successful database creation."""
        result = manager.create_database()
        assert result is True

    def test_create_database_failure(self, manager: DatabaseManager):
        """Test database creation failure."""
        with patch.object(
            manager.base.metadata, "create_all", side_effect=Exception("Test error")
        ):
            result = manager.create_database()
            assert result is False

    def test_drop_database_success(self, manager: DatabaseManager):
        """Test successful database drop."""
        manager.create_database()
        result = manager.drop_database()
        assert result is True

    def test_drop_database_failure(self, manager: DatabaseManager):
        """Test database drop failure."""
        with patch.object(
            manager.base.metadata, "drop_all", side_effect=Exception("Test error")
        ):
            result = manager.drop_database()
            assert result is False

    def test_reset_database_success(self, manager: DatabaseManager):
        """Test successful database reset."""
        manager.create_database()

        with patch.object(manager, "drop_database", return_value=True):
            with patch.object(manager, "__init__", return_value=None) as mock_init:
                manager.reset_database()
                mock_init.assert_called_once_with(manager.database_url)

    def test_reset_database_failure(self, manager: DatabaseManager):
        """Test database reset failure when drop fails."""
        with patch.object(manager, "drop_database", return_value=False):
            result = manager.reset_database()
            assert result is False

    def test_get_database_info(self, manager: DatabaseManager):
        """Test getting database information."""
        manager.create_database()
        assert manager.get_database_info() == {
            "database_url": manager.database_url,
            "tables": ["test_table_manager"],
            "table_counts": {"test_table_manager": 1},
        }

    def test_get_database_info_with_error(self, manager: DatabaseManager):
        """Test get_database_info handles errors gracefully."""
        with patch.object(manager, "get_session", side_effect=Exception("Test error")):
            assert manager.get_database_info() == {
                "database_url": manager.database_url,
                "tables": [],
                "table_counts": {},
            }

    def test_get_database_info_execute_error(self, manager: DatabaseManager):
        """Test get_database_info handles errors gracefully."""
        with patch.object(manager, "get_session") as mock_get_session:
            mock_session = mock_get_session.return_value.__enter__.return_value
            mock_session.execute.side_effect = Exception("Test error")
            assert manager.get_database_info() == {
                "database_url": manager.database_url,
                "tables": ["test_table_manager"],
                "table_counts": {"test_table_manager": "Test error"},
            }

    def test_print_database_info(self, manager, capsys):
        """Test printing database information."""
        manager.create_database()

        manager.print_database_info()

        captured = capsys.readouterr()
        assert "Database Information" in captured.out
        assert "Database URL:" in captured.out
        assert "Total Tables:" in captured.out

    def test_backup_database_sqlite_success(self, manager: DatabaseManager):
        """Test successful SQLite database backup."""
        manager.create_database()

        with tempfile.NamedTemporaryFile(suffix=".backup", delete=False) as f:
            backup_path = f.name

        try:
            result = manager.backup_database(backup_path)
            assert result is True
            assert os.path.exists(backup_path)
        finally:
            if os.path.exists(backup_path):
                os.unlink(backup_path)

    def test_backup_database_non_sqlite(self):
        """Test backup fails for non-SQLite databases."""
        with patch(
            "src.my_sqlalchemy.manager.DatabaseManager.__init__", return_value=None
        ):
            manager = DatabaseManager.__new__(DatabaseManager)
            manager.database_url = "postgresql://user:pass@localhost/db"
            result = manager.backup_database()
            assert result is False

    def test_backup_database_error(self, manager: DatabaseManager):
        """Test backup handles file errors."""
        with patch("shutil.copy2", side_effect=Exception("Test error")):
            result = manager.backup_database()
            assert result is False

    def test_restore_database_sqlite_success(self, manager: DatabaseManager):
        """Test successful SQLite database restore."""
        manager.create_database()
        with tempfile.NamedTemporaryFile(suffix=".backup", delete=False) as f:
            backup_path = f.name

        try:
            manager.backup_database(backup_path)
            result = manager.restore_database(backup_path)
            assert result is True
        finally:
            if os.path.exists(backup_path):
                os.unlink(backup_path)

    def test_restore_database_non_sqlite(self):
        """Test restore fails for non-SQLite databases."""
        with patch(
            "src.my_sqlalchemy.manager.DatabaseManager.__init__", return_value=None
        ):
            manager = DatabaseManager.__new__(DatabaseManager)
            manager.database_url = "postgresql://user:pass@localhost/db"
            result = manager.restore_database("backup.db")
            assert result is False

    def test_restore_database_error(self, manager: DatabaseManager):
        """Test restore handles file errors."""
        with patch("shutil.copy2", side_effect=Exception("Test error")):
            result = manager.restore_database("nonexistent.backup")
            assert result is False

    def test_vacuum_database_sqlite_success(self, manager: DatabaseManager):
        """Test successful SQLite database vacuum."""
        manager.create_database()

        result = manager.vacuum_database()
        assert result is True

    def test_vacuum_database_non_sqlite(self):
        """Test vacuum fails for non-SQLite databases."""
        with patch(
            "src.my_sqlalchemy.manager.DatabaseManager.__init__", return_value=None
        ):
            manager = DatabaseManager.__new__(DatabaseManager)
            manager.database_url = "postgresql://user:pass@localhost/db"
            result = manager.vacuum_database()
            assert result is False

    def test_vacuum_database_error(self, manager: DatabaseManager):
        """Test vacuum handles database errors."""
        with patch("sqlite3.connect", side_effect=Exception("Test error")):
            result = manager.vacuum_database()
            assert result is False


class TestDatabaseManagerCLI:
    def test_cli_no_command(self, capsys):
        """Test CLI with no command shows help."""
        with patch("sys.argv", ["cli"]):
            cli()
            captured = capsys.readouterr()
            assert "My SQLAlchemy Database Manager" in captured.out

    def test_cli_create_command(self, temp_db):
        """Test CLI create command."""
        with patch("sys.argv", ["cli", "--db-url", temp_db, "create"]):
            with patch(
                "src.my_sqlalchemy.manager.DatabaseManager.create_database",
                return_value=True,
            ) as mock_create:
                cli()
                mock_create.assert_called_once()

    def test_cli_drop_command_confirmed(self, temp_db):
        """Test CLI drop command with confirmation."""
        with patch("sys.argv", ["cli", "--db-url", temp_db, "drop"]):
            with patch("builtins.input", return_value="y"):
                with patch(
                    "src.my_sqlalchemy.manager.DatabaseManager.drop_database",
                    return_value=True,
                ) as mock_drop:
                    cli()
                    mock_drop.assert_called_once()

    def test_cli_drop_command_cancelled(self, temp_db, capsys):
        """Test CLI drop command cancelled."""
        with patch("sys.argv", ["cli", "--db-url", temp_db, "drop"]):
            with patch("builtins.input", return_value="n"):
                with patch(
                    "src.my_sqlalchemy.manager.DatabaseManager.drop_database"
                ) as mock_drop:
                    cli()
                    mock_drop.assert_not_called()
                    captured = capsys.readouterr()
                    assert "Operation cancelled" in captured.out

    def test_cli_reset_command_confirmed(self, temp_db):
        """Test CLI reset command with confirmation."""
        with patch("sys.argv", ["cli", "--db-url", temp_db, "reset"]):
            with patch("builtins.input", return_value="y"):
                with patch(
                    "src.my_sqlalchemy.manager.DatabaseManager.reset_database",
                    return_value=True,
                ) as mock_reset:
                    cli()
                    mock_reset.assert_called_once()

    def test_cli_reset_command_cancelled(self, temp_db, capsys):
        """Test CLI reset command cancelled."""
        with patch("sys.argv", ["cli", "--db-url", temp_db, "reset"]):
            with patch("builtins.input", return_value="n"):
                with patch(
                    "src.my_sqlalchemy.manager.DatabaseManager.reset_database"
                ) as mock_reset:
                    cli()
                    mock_reset.assert_not_called()
                    captured = capsys.readouterr()
                    assert "Operation cancelled" in captured.out

    def test_cli_info_command(self, temp_db):
        """Test CLI info command."""
        with patch("sys.argv", ["cli", "--db-url", temp_db, "info"]):
            with patch(
                "src.my_sqlalchemy.manager.DatabaseManager.print_database_info"
            ) as mock_info:
                cli()
                mock_info.assert_called_once()

    def test_cli_backup_command_with_path(self, temp_db):
        """Test CLI backup command with path."""
        with patch(
            "sys.argv", ["cli", "--db-url", temp_db, "backup", "--path", "test.backup"]
        ):
            with patch(
                "src.my_sqlalchemy.manager.DatabaseManager.backup_database",
                return_value=True,
            ) as mock_backup:
                cli()
                mock_backup.assert_called_once_with("test.backup")

    def test_cli_backup_command_without_path(self, temp_db):
        """Test CLI backup command without path."""
        with patch("sys.argv", ["cli", "--db-url", temp_db, "backup"]):
            with patch(
                "src.my_sqlalchemy.manager.DatabaseManager.backup_database",
                return_value=True,
            ) as mock_backup:
                cli()
                mock_backup.assert_called_once_with(None)

    def test_cli_restore_command_confirmed(self, temp_db):
        """Test CLI restore command with confirmation."""
        with patch("sys.argv", ["cli", "--db-url", temp_db, "restore", "test.backup"]):
            with patch("builtins.input", return_value="y"):
                with patch(
                    "src.my_sqlalchemy.manager.DatabaseManager.restore_database",
                    return_value=True,
                ) as mock_restore:
                    cli()
                    mock_restore.assert_called_once_with("test.backup")

    def test_cli_restore_command_cancelled(self, temp_db, capsys):
        """Test CLI restore command cancelled."""
        with patch("sys.argv", ["cli", "--db-url", temp_db, "restore", "test.backup"]):
            with patch("builtins.input", return_value="n"):
                with patch(
                    "src.my_sqlalchemy.manager.DatabaseManager.restore_database"
                ) as mock_restore:
                    cli()
                    mock_restore.assert_not_called()
                    captured = capsys.readouterr()
                    assert "Operation cancelled" in captured.out

    def test_cli_vacuum_command(self, temp_db):
        """Test CLI vacuum command."""
        with patch("sys.argv", ["cli", "--db-url", temp_db, "vacuum"]):
            with patch(
                "src.my_sqlalchemy.manager.DatabaseManager.vacuum_database",
                return_value=True,
            ) as mock_vacuum:
                cli()
                mock_vacuum.assert_called_once()

    def test_cli_default_database_url(self):
        """Test CLI uses default database URL when not specified."""
        with patch("sys.argv", ["cli", "create"]):
            with patch("src.my_sqlalchemy.manager.DatabaseManager") as mock_manager:
                cli()
                mock_manager.assert_called_once_with("sqlite:///my_sqlalchemy.db")
