from datetime import UTC, datetime


def utc_now():
    """Return current UTC timestamp. Callable for SQLAlchemy default values."""
    return datetime.now(UTC)


def get_values_from_model_instance(model_instance):
    data = []
    for column in model_instance.__table__.columns:
        value = getattr(model_instance, column.name)
        if isinstance(value, datetime):
            value = value.isoformat()
        data.append(f"{column.name}={value}")
    return f"<{', '.join(data)}>"


def get_columns_detail_from_model(model_class):
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


def get_columns_type_from_model(model_class):
    columns = {
        column.name: str(column.type) for column in model_class.__table__.columns
    }
    return columns
