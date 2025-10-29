from sqlalchemy import Column, DateTime, Integer
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
