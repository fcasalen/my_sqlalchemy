from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, delete, func, select, update
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.sql.selectable import Select

from . import utils
from .base import Base
from .models_handler import ModelsHandler


class MySQLAlchemy:
    """A simple sqlalchemy wrapper"""

    def __init__(self, database_url: str, models: list[DeclarativeMeta]):
        """Initialize the service with database connection.

        Args:
            database_url (str): The database connection URL.
            models (list[DeclarativeMeta]): List of model classes to be managed. All models must inherit from StandardModel.
        """
        self.database_url = database_url
        self.models = ModelsHandler(models_list=models)
        self.base = Base
        self.engine = create_engine(
            self.database_url,
            echo=False,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False}
            if "sqlite" in self.database_url
            else {},
        )
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.base.metadata.create_all(self.engine)

    @contextmanager
    def get_session(self):
        """Context manager for database sessions."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _expunge_object(self, session: Session, obj: Any) -> Any:
        """Helper to expunge single object from session."""
        if obj:
            # Force loading of lazy-loaded attributes
            self._force_load_attributes(obj)
            session.expunge(obj)
        return obj

    def _expunge_objects(self, session: Session, objects: list) -> list:
        """Helper to expunge list of objects from session."""
        for obj in objects:
            self._force_load_attributes(obj)
            session.expunge(obj)
        return objects

    def _force_load_attributes(self, obj: Any) -> None:
        """Force loading of commonly accessed attributes to prevent lazy loading issues."""
        # This is a generic implementation - subclasses can override for specific entities
        try:
            _ = getattr(obj, "id", None)
            _ = getattr(obj, "created_at", None)
            _ = getattr(obj, "updated_at", None)
        except Exception:
            pass

    def _assert_model(self, model) -> None:
        """Assert that the provided model is valid.

        Args:
            model (DeclarativeMeta): The model class to validate

        Raises:
            UnmappedError: If the provided model is not mapped.
        """
        assert model in self.models.models_list, (
            f"The model {model} is not mapped. Valid models are: {self.models.models_list_names}.\n\n"
            "Use self.models.all_models() to check valid models."
        )

    def _construct_where_clause(
        self, stmt: Select, model: DeclarativeMeta, query_kwargs: dict
    ) -> Select:
        for column_name, value in query_kwargs.items():
            column = getattr(model, column_name)
            stmt = stmt.where(column == value)
        return stmt

    def add(self, model: DeclarativeMeta, data: list[dict]) -> dict:
        """add one or more entities.

        Args:
            model (DeclarativeMeta): The model class to add instances of. Use self.models.all_models() method to check valid models.
            data (list[dict]): List of dictionaries with column-value mappings for bulk creation. Use self.models.<model_name>.get_columns() to check valid columns.

        Raises:
            UnmappedError: If the provided model is not mapped.
            TypeError: If invalid columns are provided in the data dictionaries.
        """
        self._assert_model(model)
        with self.get_session() as session:
            new_entities = [model(**item_data) for item_data in data]
            session.add_all(new_entities)
            session.flush()
        print("✅Data added successfully.")

    def delete(self, model: DeclarativeMeta, **query_kwargs) -> int:
        """Delete an entity. To be implemented by subclasses.

        Args:
            model: The model class to delete from. Use self.models.all_models() method to check valid models.
            **query_kwargs: Filter criteria for deletion. Use self.models.<model_name>.get_columns() to check valid columns.

        Returns:
            int: The number of rows deleted.
        """
        self._assert_model(model)
        stmt = delete(model)
        stmt = self._construct_where_clause(stmt, model, query_kwargs)
        with self.get_session() as session:
            result = session.execute(stmt)
            session.commit()
            return result.rowcount

    def get(self, model, limit: int = None, **query_kwargs) -> list[dict]:
        """Find an entity. To be implemented by subclasses.

        Args:
            model: The model class to search in. Use self.models.all_models() method to check valid models.
            limit (int, optional): Maximum number of results to return. Defaults to None (no limit).
            **query_kwargs: Filter criteria for finding. Use self.models.<model_name>.get_columns() to check valid columns.

        Returns:
            list[dict]: List of found entity instances (dictionaries).
        """
        self._assert_model(model)
        stmt = select(model)
        stmt = self._construct_where_clause(stmt, model, query_kwargs)
        with self.get_session() as session:
            if limit:
                stmt = stmt.limit(limit)
            result = session.execute(stmt).scalars().all()
            return self._expunge_objects(session, result)

    def update(self, model, data_to_be_updated: dict, **query_kwargs) -> int:
        """Update an entity. To be implemented by subclasses.

        Args:
            model: The model class to update. Use self.models.all_models() method to check valid models.
            data_to_be_updated (dict): Dictionary of column-value pairs to be updated.
            **kwargs: Update criteria and values. Use self.models.<model_name>.get_columns() to check valid columns.

        Returns:
            int: The number of rows updated.
        """
        self._assert_model(model)
        stmt = update(model)
        data_to_be_updated["updated_at"] = utils.utc_now()
        stmt = self._construct_where_clause(stmt, model, query_kwargs)
        stmt = stmt.values(**data_to_be_updated)
        with self.get_session() as session:
            result = session.execute(stmt)
            session.commit()
            return result.rowcount

    def count(self, model, **query_kwargs) -> int:
        """Count entities matching criteria.

        Args:
            model: The model class to count in. Use self.models.all_models() method to check valid models.
            **query_kwargs: Filter criteria for counting. Use self.models.<model_name>.get_columns() to check valid columns.

        Returns:
            int: The count of matching entities.
        """
        self._assert_model(model)
        stmt = select(func.count()).select_from(model)
        stmt = self._construct_where_clause(stmt, model, query_kwargs)
        with self.get_session() as session:
            result = session.execute(stmt).scalar()
            return result
