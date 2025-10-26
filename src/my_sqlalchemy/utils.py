from datetime import UTC, datetime
from typing import Any
from sqlalchemy.orm.decl_api import DeclarativeMeta


def utc_now():
    """Return current UTC timestamp. Callable for SQLAlchemy default values."""
    return datetime.now(UTC)


def get_values_from_model_instance(model_instance: DeclarativeMeta) -> dict[str, Any]:
    """Get columns as keys and their values from a model instance as values in a dictionary.
    Datetime values are converted to ISO format strings.

    Args:
        model_instance (DeclarativeMeta): The model instance to extract values from.

    Returns:
        dict[str, Any]: A dictionary with column names as keys and their values as values.
    """
    data = {}
    for column in model_instance.__table__.columns:
        value = getattr(model_instance, column.name)
        if isinstance(value, datetime):
            value = value.isoformat()
        data[column.name] = value
    return data


def get_columns_detail_from_model(
    model_class: DeclarativeMeta,
) -> dict[str, dict[str, Any]]:
    """Get columns as key and their detail (type, nullable, etc) as value in a dictionary, excluding primary key.

    Args:
        model_class (DeclarativeMeta): The model class to extract columns from.

    Returns:
        dict[str, dict[str, Any]]: A dictionary with column names as keys and their details as values.
    """
    columns = {
        column.name: {
            "type": str(column.type),
            "length": getattr(column.type, "length", None),
            "nullable": column.nullable,
            "default": column.default.arg if column.default is not None else None,
        }
        for column in model_class.__table__.columns
        if not column.primary_key
    }
    for k in columns:
        if callable(columns[k]["default"]):
            columns[k]["default"] = f"function {columns[k]['default'].__name__}"
    return columns


def get_columns_type_from_model(model_class: DeclarativeMeta) -> dict[str, str]:
    """Get columns names as keys and their types as values in a dictionary.

    Args:
        model_class (DeclarativeMeta): The model class to extract columns from.

    Returns:
        dict[str, str]: A dictionary with column names as keys and their types as values.
    """
    columns = {
        column.name: str(column.type) for column in model_class.__table__.columns
    }
    return columns
