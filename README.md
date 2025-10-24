# My SQLAlchemy

A simplified SQLAlchemy wrapper that provides an easy-to-use interface for database operations with built-in model validation and management utilities.

## Features

- 🚀 **Simple Database Operations**: Easy CRUD operations with a clean API
- 🔍 **Model Validation**: Automatic validation of SQLAlchemy models
- 📊 **Database Management**: Built-in tools for database backup, restore, and maintenance
- �️ **Command Line Interface**: Full-featured CLI for database management operations
- 🛡�️ **Type Safety**: Full type hints support for better development experience
- 🧪 **Well Tested**: Comprehensive test suite with high coverage
- 📈 **Performance**: Optimized queries with proper connection management

## Installation

```bash
pip install my_sqlalchemy
```

This will install the library and make the `my_sqlalchemy_manager` CLI command available.

## Quick Start

### Basic Usage

```python
from sqlalchemy import Column, Integer, String
from my_sqlalchemy import MySQLAlchemy, StandardModel

# Define your model
class User(StandardModel):
    __tablename__ = 'users'

    name = Column(String(50))
    email = Column(String(100))

# Initialize the database connection
db = MySQLAlchemy("sqlite:///example.db", models=[User])

# Add data
users_data = [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"}
]
db.add(User, users_data)

# Query data
all_users = db.get(User)
specific_user = db.get(User, name="Alice")
limited_results = db.get(User, limit=5)

# Count records
total_users = db.count(User)
alice_count = db.count(User, name="Alice")

# Update data
updated_rows = db.update(User, {"email": "alice.new@example.com"}, name="Alice")

# Delete data
deleted_rows = db.delete(User, name="Bob")
```

### Using the Database Manager

```python
from my_sqlalchemy import DatabaseManager

# Initialize manager
manager = DatabaseManager("sqlite:///example.db")

# Create database
manager.create_database()

# Get database information
info = manager.get_database_info()
print(f"Tables: {info['tables']}")
print(f"Table counts: {info['table_counts']}")

# Backup database (SQLite only)
manager.backup_database("backup.db")

# Restore database (SQLite only)
manager.restore_database("backup.db")

# Vacuum database (SQLite only)
manager.vacuum_database()

# Reset database (drop and recreate)
manager.reset_database()
```

### Command Line Interface (CLI)
```bash
# Show help and available commands
my_sqlalchemy_manager --help

# Show help for a specific command
my_sqlalchemy_manager backup --help

# Create database and tables
my_sqlalchemy_manager create

# Drop all tables (with confirmation prompt)
my_sqlalchemy_manager drop

# Reset database - drop and recreate all tables (with confirmation prompt)
my_sqlalchemy_manager reset

# Show database information (tables, record counts, etc.)
my_sqlalchemy_manager info

# Backup database to default location
my_sqlalchemy_manager backup

# Backup database to specific path
my_sqlalchemy_manager backup --path /path/to/backup.db

# Restore database from backup (with confirmation prompt)
my_sqlalchemy_manager restore /path/to/backup.db

# Vacuum database to optimize storage (SQLite only)
my_sqlalchemy_manager vacuum

# Use custom SQLite database
my_sqlalchemy_manager --db-url sqlite:///my_custom.db info

# Use PostgreSQL database
my_sqlalchemy_manager --db-url postgresql://user:pass@localhost/mydb info

# Use MySQL database
my_sqlalchemy_manager --db-url mysql://user:pass@localhost/mydb info
```

### Safety Features

The CLI includes built-in safety features:

- **Confirmation prompts** for destructive operations (drop, reset, restore)
- **Clear operation status** with ✅ success and ❌ error indicators
- **Automatic error handling** with descriptive error messages
- **Help documentation** for all commands and options

### StandardModel Class

Base model class with common fields and utilities.

#### Features

- `id`: Auto-incrementing primary key
- `created_at`: Timestamp of creation
- `updated_at`: Timestamp of last update (auto-updated)

#### Methods

##### `get_columns()`
Get column details as a dictionary.

```python
columns = User.get_columns()
```

##### `get_columns_type()`
Get column types as a dictionary.

```python
column_types = User.get_columns_type()
```

## Project Structure

```
my_sqlalchemy/
├── src/
│   └── my_sqlalchemy/
│       ├── __init__.py
│       ├── base.py              # Declarative base
│       ├── manager.py           # Database management utilities
│       ├── models_handler.py    # Model validation and handling
│       ├── my_sqlalchemy.py     # Main MySQLAlchemy class
│       ├── standard_model.py    # Base model with common fields
│       └── utils.py             # Utility functions
├── tests/
│   ├── test_manager.py          # Database manager tests
│   ├── test_models_handler.py   # Model handler tests
│   ├── test_my_sqlalchemy.py    # Main class tests
│   └── test_standard_model.py   # Standard model tests
├── pyproject.toml
└── README.md
```

## Development

### Setting up the development environment

```bash
# Clone the repository
git clone <repository-url>
cd my_sqlalchemy

# Install in development mode
pip install -e .

# Install development dependencies
pip install -e .[dev]
```

### Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_my_sqlalchemy.py

# Run with coverage
pytest --cov=src/my_sqlalchemy
```

## Supported Databases

- SQLite (full support including backup/restore)
- PostgreSQL
- MySQL
- Other SQLAlchemy-supported databases

Note: Database management features (backup, restore, vacuum) are currently SQLite-specific.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`pytest`)
6. Commit your changes (`git commit -m 'chd: Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

**Fernando Casale Neto**
- Email: fcasalen@gmail.com

## Changelog

### 0.1.0
- Initial release
- Basic CRUD operations (add, get, count, update, delete)
- Model validation and handling
- Database management utilities
- Command-line interface for database operations
- Support for SQLite, PostgreSQL, MySQL, and other SQLAlchemy-supported databases
- Comprehensive test suite with high coverage
- Built-in backup, restore, and vacuum operations for SQLite
