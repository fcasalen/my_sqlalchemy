from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, delete, func, select, update
from sqlalchemy.orm import sessionmaker
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
        """Context manager for database sessions.
        It yields a session and ensures commit/rollback and closure.

        Yields:
            Session: A SQLAlchemy session object.
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _assert_model(self, model) -> None:
        """Assert that the provided model is valid.

        Args:
            model (DeclarativeMeta): The model class to validate

        Raises:
            AssertionError: If the provided model is not mapped.
        """
        assert model in self.models.models_list, (
            f"The model {model} is not mapped. Valid models are: {self.models.models_list_names}.\n\n"
            "Use self.models.all_models() to check valid models."
        )

    def _construct_where_clause(
        self, stmt: Select, model: DeclarativeMeta, query_kwargs: dict
    ) -> Select:
        """Construct the where clause to be used in a select

        Args:
            stmt (Select): The initial select statement
            model (DeclarativeMeta): The model class to build the where clause for
            query_kwargs (dict): The filter criteria as column-value pairs

        Returns:
            Select: The select statement with the constructed where clause
        """
        for column_name, value in query_kwargs.items():
            column = getattr(model, column_name)
            stmt = stmt.where(column == value)
        return stmt

    def add(self, model: DeclarativeMeta, data: list[dict]) -> dict[str, Any]:
        """add one or more entities.

        Args:
            model (DeclarativeMeta): The model class to add instances of. Use self.models.all_models() method to check valid models.
            data (list[dict]): List of dictionaries with column-value mappings for bulk creation. Use self.models.<model_name>.get_columns() to check valid columns.

        Raises:
            AssertionError: If the provided model is not mapped.

        Returns:
            dict[str, Any]: A dictionary indicating success or failure. In case of failure, includes an error message.
        """
        self._assert_model(model)
        try:
            with self.get_session() as session:
                new_entities = [model(**item_data) for item_data in data]
                session.add_all(new_entities)
                session.flush()
                return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

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
            return result.rowcount

    def get(
        self,
        model,
        limit: int = None,
        columns_to_return: list[str] = None,
        **query_kwargs,
    ) -> list[dict]:
        """Find an entity. To be implemented by subclasses.

        Args:
            model: The model class to search in. Use self.models.all_models() method to check valid models.
            limit (int, optional): Maximum number of results to return. Defaults to None (no limit).
            columns_to_return (list[str], optional): List of column names to return. Defaults to None (all columns).
            **query_kwargs: Filter criteria for finding. Use self.models.<model_name>.get_columns() to check valid columns.

        Returns:
            list[dict]: List of found entity instances (dictionaries).
        """
        self._assert_model(model)

        if columns_to_return is None:
            columns_to_return = model.get_columns_type().keys()

        columns = [getattr(model, col) for col in columns_to_return]
        stmt = select(*columns)
        stmt = self._construct_where_clause(stmt, model, query_kwargs)

        with self.get_session() as session:
            if limit:
                stmt = stmt.limit(limit)

            result = session.execute(stmt)
            rows = result.all()
            return [dict(zip(columns_to_return, row)) for row in rows]

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
