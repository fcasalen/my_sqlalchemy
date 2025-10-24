from sqlalchemy import Column, DateTime, Integer

from . import utils
from .base import Base


class StandardModel(Base):
    """A standard model with common columns for reuse."""

    __abstract__ = True  # Mark this class as abstract, so it won't create a table

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=utils.utc_now, nullable=False)
    updated_at = Column(
        DateTime, default=utils.utc_now, onupdate=utils.utc_now, nullable=False
    )

    def __repr__(self):
        """String representation of the model instance."""
        return utils.get_values_from_model_instance(self)

    @classmethod
    def get_columns(cls):
        """Get the columns of the table as a dictionary excluding the primary key."""
        return utils.get_columns_detail_from_model(cls)

    @classmethod
    def get_columns_type(cls):
        """Get the columns types of the table as a dictionary."""
        return utils.get_columns_type_from_model(cls)
