import argparse
import shutil
import sqlite3
from typing import Optional

from sqlalchemy import text

from .my_sqlalchemy import MySQLAlchemy


class DatabaseManager(MySQLAlchemy):
    """Database management utilities."""

    def __init__(self, database_url: str):
        """Initialize the database manager."""
        super().__init__(database_url=database_url)

    def create_database(self) -> bool:
        """Create all tables in the database."""
        try:
            self.base.metadata.create_all(self.engine)
            print("✅ Database created successfully!")
            return True
        except Exception as e:
            print(f"❌ Error creating database: {e}")
            return False

    def drop_database(self) -> bool:
        """Drop all tables in the database."""
        try:
            self.base.metadata.drop_all(self.engine)
            print("✅ Database dropped successfully!")
            return True
        except Exception as e:
            print(f"❌ Error dropping database: {e}")
            return False

    def reset_database(self) -> bool:
        """Drop and recreate the database."""
        print("🔄 Resetting database...")
        if self.drop_database():
            return self.__init__(self.database_url)
        return False

    def get_database_info(self) -> dict:
        """Get information about the database."""
        info = {"database_url": self.database_url, "tables": [], "table_counts": {}}
        try:
            info["tables"] = list(self.base.metadata.tables.keys())
            with self.get_session() as session:
                for table_name in info["tables"]:
                    try:
                        result = session.execute(
                            text(f"SELECT COUNT(*) FROM {table_name}")
                        )
                        count = result.scalar()
                        info["table_counts"][table_name] = count
                    except Exception as e:
                        info["table_counts"][table_name] = str(e)
        except Exception as e:
            print(f"❌ Error getting database info: {e}")
            info["tables"] = []
            info["table_counts"] = {}
        return info

    def print_database_info(self):
        """Print database information."""
        info = self.get_database_info()
        print("\n📊 Database Information")
        print(f"{'=' * 50}")
        print(f"Database URL: {info['database_url']}")
        print(f"Total Tables: {len(info['tables'])}")
        print("\nTable Row Counts:")
        print(f"{'-' * 30}")
        for table_name in sorted(info["tables"]):
            count = info["table_counts"].get(table_name, 0)
            print(f"  {table_name:<35} {count:>10}")
        total_rows = sum(
            count for count in info["table_counts"].values() if isinstance(count, int)
        )
        print(f"{'-' * 30}")
        print(f"  {'Total Rows':<35} {total_rows:>10}")

    def backup_database(self, backup_path: Optional[str] = None) -> bool:
        """Backup the SQLite database."""
        if not self.database_url.startswith("sqlite:///"):
            print("❌ Backup only supported for SQLite databases")
            return False
        try:
            db_path = self.database_url.replace("sqlite:///", "")
            if not backup_path:
                backup_path = f"{db_path}.backup"
            shutil.copy2(db_path, backup_path)
            print(f"✅ Database backed up to: {backup_path}")
            return True
        except Exception as e:
            print(f"❌ Error backing up database: {e}")
            return False

    def restore_database(self, backup_path: str) -> bool:
        """Restore the SQLite database from backup."""
        if not self.database_url.startswith("sqlite:///"):
            print("❌ Restore only supported for SQLite databases")
            return False
        try:
            db_path = self.database_url.replace("sqlite:///", "")
            shutil.copy2(backup_path, db_path)
            print(f"✅ Database restored from: {backup_path}")
            return True
        except Exception as e:
            print(f"❌ Error restoring database: {e}")
            return False

    def vacuum_database(self) -> bool:
        """Vacuum the SQLite database to optimize storage."""
        if not self.database_url.startswith("sqlite:///"):
            print("❌ Vacuum only supported for SQLite databases")
            return False
        try:
            db_path = self.database_url.replace("sqlite:///", "")
            conn = sqlite3.connect(db_path)
            conn.execute("VACUUM")
            conn.close()
            print("✅ Database vacuumed successfully!")
            return True
        except Exception as e:
            print(f"❌ Error vacuuming database: {e}")
            return False


def cli():
    """Command-line interface for database management."""
    parser = argparse.ArgumentParser(description="My SQLAlchemy Database Manager")
    parser.add_argument(
        "--db-url",
        default="sqlite:///my_sqlalchemy.db",
        help="Database URL (default: sqlite:///my_sqlalchemy.db)",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    subparsers.add_parser("create", help="Create database and tables")
    subparsers.add_parser("drop", help="Drop all tables")
    subparsers.add_parser("reset", help="Drop and recreate database")
    subparsers.add_parser("info", help="Show database information")
    backup_parser = subparsers.add_parser("backup", help="Backup database")
    backup_parser.add_argument("--path", help="Backup file path")
    restore_parser = subparsers.add_parser("restore", help="Restore database")
    restore_parser.add_argument("path", help="Backup file path to restore from")
    subparsers.add_parser("vacuum", help="Vacuum database")
    subparsers.add_parser("init-package", help="Create __init__.py file")
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    manager = DatabaseManager(args.db_url)
    if args.command == "create":
        manager.create_database()
    elif args.command == "drop":
        confirm = input("⚠️  Are you sure you want to drop all tables? (y/N): ")
        if confirm.lower() == "y":
            manager.drop_database()
        else:
            print("❌ Operation cancelled")
    elif args.command == "reset":
        confirm = input(
            "⚠️  Are you sure you want to reset the entire database? (y/N): "
        )
        if confirm.lower() == "y":
            manager.reset_database()
        else:
            print("❌ Operation cancelled")
    elif args.command == "info":
        manager.print_database_info()
    elif args.command == "backup":
        manager.backup_database(args.path)
    elif args.command == "restore":
        confirm = input(
            f"⚠️  Are you sure you want to restore from {args.path}? This will overwrite the current database. (y/N): "
        )
        if confirm.lower() == "y":
            manager.restore_database(args.path)
        else:
            print("❌ Operation cancelled")
    elif args.command == "vacuum":
        manager.vacuum_database()
    manager.engine.dispose()
