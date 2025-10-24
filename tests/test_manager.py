import os
import tempfile
import time
from unittest.mock import patch

import pytest

from src.my_sqlalchemy.manager import DatabaseManager


@pytest.fixture
def temp_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db_url = f"sqlite:///{db_path}"
    yield db_url
    # Cleanup with retry mechanism for Windows
    max_retries = 5
    for i in range(max_retries):
        try:
            if os.path.exists(db_path):
                os.unlink(db_path)
            break
        except PermissionError:
            if i < max_retries - 1:
                time.sleep(0.1)  # Brief wait before retry
            else:
                # If all retries fail, just pass - temp files will be cleaned up by OS
                pass


@pytest.fixture
def manager(temp_db):
    """Create a DatabaseManager instance with temporary database."""
    manager_instance = DatabaseManager(temp_db)
    yield manager_instance
    # Properly close all database connections
    try:
        manager_instance.engine.dispose()
    except Exception:
        pass


class TestDatabaseManager:
    def test_init(self, temp_db):
        """Test DatabaseManager initialization."""
        manager = DatabaseManager(temp_db)
        assert manager.database_url == temp_db
        assert hasattr(manager, "base")
        assert hasattr(manager, "engine")

    def test_drop_database_success(self, manager):
        """Test successful database drop."""
        # First create the database
        manager.create_database()

        # Then drop it
        result = manager.drop_database()
        assert result is True

        # Ensure all connections are properly closed
        manager.engine.dispose()

    def test_drop_database_failure(self, manager):
        """Test database drop failure."""
        with patch.object(
            manager.base.metadata, "drop_all", side_effect=Exception("Test error")
        ):
            result = manager.drop_database()
            assert result is False

    def test_reset_database_success(self, manager):
        """Test successful database reset."""
        manager.create_database()

        with patch.object(manager, "drop_database", return_value=True):
            with patch.object(manager, "__init__", return_value=None) as mock_init:
                manager.reset_database()
                mock_init.assert_called_once_with(manager.database_url)

        # Clean up connections
        manager.engine.dispose()

    def test_reset_database_failure(self, manager):
        """Test database reset failure when drop fails."""
        with patch.object(manager, "drop_database", return_value=False):
            result = manager.reset_database()
            assert result is False

    def test_get_database_info(self, manager):
        """Test getting database information."""
        manager.create_database()

        info = manager.get_database_info()

        assert "database_url" in info
        assert "tables" in info
        assert "table_counts" in info
        assert info["database_url"] == manager.database_url
        assert isinstance(info["tables"], list)
        assert isinstance(info["table_counts"], dict)

        # Clean up connections
        manager.engine.dispose()

    def test_get_database_info_with_error(self, manager):
        """Test get_database_info handles errors gracefully."""
        with patch.object(manager, "get_session", side_effect=Exception("Test error")):
            info = manager.get_database_info()
            assert info["tables"] == []
            assert info["table_counts"] == {}

    def test_print_database_info(self, manager, capsys):
        """Test printing database information."""
        manager.create_database()

        manager.print_database_info()

        captured = capsys.readouterr()
        assert "Database Information" in captured.out
        assert "Database URL:" in captured.out
        assert "Total Tables:" in captured.out

        # Clean up connections
        manager.engine.dispose()

    def test_backup_database_sqlite_success(self, manager):
        """Test successful SQLite database backup."""
        manager.create_database()

        with tempfile.NamedTemporaryFile(suffix=".backup", delete=False) as f:
            backup_path = f.name

        try:
            result = manager.backup_database(backup_path)
            assert result is True
            assert os.path.exists(backup_path)
        finally:
            # Clean up connections before file cleanup
            manager.engine.dispose()
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

    def test_backup_database_error(self, manager):
        """Test backup handles file errors."""
        with patch("shutil.copy2", side_effect=Exception("Test error")):
            result = manager.backup_database()
            assert result is False

    def test_restore_database_sqlite_success(self, manager):
        """Test successful SQLite database restore."""
        manager.create_database()

        # Create a backup first
        with tempfile.NamedTemporaryFile(suffix=".backup", delete=False) as f:
            backup_path = f.name

        try:
            manager.backup_database(backup_path)
            result = manager.restore_database(backup_path)
            assert result is True
        finally:
            # Clean up connections before file cleanup
            manager.engine.dispose()
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

    def test_restore_database_error(self, manager):
        """Test restore handles file errors."""
        with patch("shutil.copy2", side_effect=Exception("Test error")):
            result = manager.restore_database("nonexistent.backup")
            assert result is False

    def test_vacuum_database_sqlite_success(self, manager):
        """Test successful SQLite database vacuum."""
        manager.create_database()

        result = manager.vacuum_database()
        assert result is True

        # Clean up connections
        manager.engine.dispose()

    def test_vacuum_database_non_sqlite(self):
        """Test vacuum fails for non-SQLite databases."""
        with patch(
            "src.my_sqlalchemy.manager.DatabaseManager.__init__", return_value=None
        ):
            manager = DatabaseManager.__new__(DatabaseManager)
            manager.database_url = "postgresql://user:pass@localhost/db"
            result = manager.vacuum_database()
            assert result is False

    def test_vacuum_database_error(self, manager):
        """Test vacuum handles database errors."""
        with patch("sqlite3.connect", side_effect=Exception("Test error")):
            result = manager.vacuum_database()
            assert result is False
