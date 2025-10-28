from sqlalchemy import Column, DateTime, Integer
from typing import Any
from datetime import datetime

from . import utils
from .base import Base


class StandardModel(Base):
    """A standard model with common columns for reuse."""

    __abstract__ = True

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=utils.utc_now, nullable=False)
    updated_at = Column(
        DateTime, default=utils.utc_now, onupdate=utils.utc_now, nullable=False
    )

    def __repr__(self) -> str:
        """String representation of the model instance."""
        data = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            if isinstance(value, datetime):
                value = value.isoformat()
            data[column.name] = value
        data = [f"{k}={v}" for k, v in data.items()]
        return f"<{', '.join(data)}>"

    @classmethod
    def get_columns(cls) -> dict[str, dict[str, Any]]:
        """Get columns as key and their detail (type, nullable, etc) as value in a dictionary.

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
            for column in cls.__table__.columns
        }
        for k in columns:
            if callable(columns[k]["default"]):
                columns[k]["default"] = f"function {columns[k]['default'].__name__}"
        return columns
