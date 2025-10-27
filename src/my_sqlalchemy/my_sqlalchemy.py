from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, delete, func, select, update, asc, desc
from sqlalchemy.orm import sessionmaker, make_transient
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
                return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def add_model_instances(
        self, model_instances: list[DeclarativeMeta]
    ) -> dict[str, Any]:
        """Add model instances.

        Args:
            model_instance (list[DeclarativeMeta]): The model instances to add.

        Raises:
            AssertionError: If any model instance already has a primary key (id) set.

        Returns:
            dict[str, Any]: A dictionary indicating success or failure. In case of failure, includes an error message.
        """
        for model_instance in model_instances:
            self._assert_model(type(model_instance))
            assert not model_instance.id, (
                "Model instance must not have a primary key (id) set when adding a new instance."
            )
        try:
            with self.get_session() as session:
                session.add_all(model_instances)
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
        columns_to_order_by: dict[str, bool] = None,
        return_objects: bool = False,
        **query_kwargs,
    ) -> list[dict]:
        """Find an entity. To be implemented by subclasses.

        Args:
            model: The model class to search in. Use self.models.all_models() method to check valid models.
            limit (int, optional): Maximum number of results to return. Defaults to None (no limit).
            columns_to_return (list[str], optional): List of column names to return. Defaults to None (all columns).
            columns_to_order_by (dict[str, bool], optional): Dictionary of column names to order the results by and a boolean indicating ascending (True) or descending (False) order. Defaults to None (no specific order).
            return_objects (bool, optional): Whether to return ORM objects instead of dictionaries. Defaults to False.
            **query_kwargs: Filter criteria for finding. Use self.models.<model_name>.get_columns() to check valid columns.

        Returns:
            list[dict]: List of found entity instances (dictionaries).
        """
        self._assert_model(model)

        if columns_to_return is None:
            columns_to_return = model.get_columns_type().keys()
        if not return_objects:
            columns = [getattr(model, col) for col in columns_to_return]
            stmt = select(*columns)
        else:
            stmt = select(model)

        stmt = self._construct_where_clause(stmt, model, query_kwargs)
        if limit:
            stmt = stmt.limit(limit)
        if columns_to_order_by:
            order_by_columns = [
                asc(getattr(model, col)) if asc_desc else desc(getattr(model, col))
                for col, asc_desc in columns_to_order_by.items()
            ]
            stmt = stmt.order_by(*order_by_columns)
        with self.get_session() as session:
            result = session.execute(stmt)
            if return_objects:
                rows = result.scalars().all()
                for row in rows:
                    make_transient(row)
                return rows
            rows = result.all()
            return [dict(zip(columns_to_return, row)) for row in rows]

    def update(self, model, data_to_be_updated: dict, **query_kwargs) -> int:
        """Update entities matching criteria.

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

    def update_model_instance(self, model_instance: DeclarativeMeta) -> dict[str, Any]:
        """Update a single model instance.

        Args:
            model_instance (DeclarativeMeta): The model instance to update.

        Raises:
            AssertionError: If the model instance does not have a valid primary key (id) or does not exist in the database.

        Returns:
            dict[str, Any]: A dictionary indicating success or failure. In case of failure, includes an error message.
        """
        self._assert_model(type(model_instance))
        assert model_instance.id, (
            "Model instance must have a valid primary key (id) to be updated."
        )
        assert self.get(type(model_instance), id=model_instance.id), (
            "Model instance does not exist. Use add_model_instances to add new instances."
        )
        try:
            with self.get_session() as session:
                model_instance.updated_at = utils.utc_now()
                session.merge(model_instance)
                return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

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
