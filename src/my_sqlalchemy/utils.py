from datetime import UTC, datetime


def utc_now():
    """Return current UTC timestamp. Callable for SQLAlchemy default values."""
    return datetime.now(UTC)
