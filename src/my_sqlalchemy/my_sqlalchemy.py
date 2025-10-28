from contextlib import contextmanager
from typing import Any

from sqlalchemy import (
    create_engine,
    delete,
    func,
    select as sql_select,
    update,
    asc as sql_asc,
    desc as sql_desc,
    UnaryExpression,
)
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

    def _assert_columns_in_model(
        self,
        model: DeclarativeMeta,
        columns: list[str],
        exclude_primary_key: bool = False,
        msg: str = "",
    ) -> None:
        """Assert that the provided columns are valid for the given model.

        Args:
            model (DeclarativeMeta): The model class to validate columns against.
            columns (list[str]): The list of column names to validate.
            exclude_primary_key (bool): Whether to exclude the primary key from validation. Defaults to False.
            msg (str): Additional message to include in the assertion error. Defaults to an empty string.

        Raises:
            AssertionError: If any of the provided columns are not valid for the model.
        """
        pk_column = None
        possible_columns = model.get_columns().keys()
        columns_to_check = columns
        if exclude_primary_key:
            pk_column = model.__table__.primary_key.columns.values()[0].name
            columns_to_check = [col for col in columns if col != pk_column]
            possible_columns = [col for col in possible_columns if col != pk_column]
        assert set(columns_to_check).issubset(set(possible_columns)), (
            f"Invalid columns {columns_to_check} for model '{model.__name__}'. Valid columns are: {possible_columns}"
        )
        if not exclude_primary_key:
            return
        pk_error = False
        if isinstance(model, DeclarativeMeta):
            if pk_column in columns:
                pk_error = True
        else:
            if getattr(model, pk_column) is not None:
                pk_error = True
        if pk_error:
            raise AssertionError(
                f"Primary key column '{pk_column}' cannot be included{msg}"
            )

    def construct_where_clause(
        self,
        stmt: Select,
        model: DeclarativeMeta,
        query_kwargs: dict,
        assert_model_columns: bool = True,
    ) -> Select:
        """Construct the where clause to be used in a select

        Args:
            stmt (Select): The initial select statement
            model (DeclarativeMeta): The model class to build the where clause for
            query_kwargs (dict): The filter criteria as column-value pairs
            assert_model_columns (bool): Whether to assert the model and column validity. Defaults to True. Use this as true when calling the method directly if the model and columns haven't been asserted before. Methods get, add, delete, update and count already asserts the models and columns.

        Returns:
            Select: The select statement with the constructed where clause
        """
        if assert_model_columns:
            self._assert_model(model)
            self._assert_columns_in_model(model, list(query_kwargs.keys()))
        for column_name, value in query_kwargs.items():
            column = getattr(model, column_name)
            stmt = stmt.where(column == value)
        return stmt

    def select(
        self,
        model: DeclarativeMeta | Any,
        columns_to_select: list[str] = None,
        assert_model: bool = True,
    ) -> Select:
        """Create a select statement for the given model.

        Args:
            model (DeclarativeMeta|Any): The model class to create a select statement for. Use self.models.all_models() method to check valid models.
            columns_to_select (list[str], optional): List of column names to select. Defaults to None (all columns).
            assert_model (bool): Whether to assert the model validity. Defaults to True. Use this as true when calling the method directly if the model hasn't been asserted before. Methods get, add, delete, update and count already asserts the models.

        Returns:
            Select: A SQLAlchemy select statement for the given model.
        """
        if assert_model:
            self._assert_model(model)
        if not columns_to_select:
            if not isinstance(model, DeclarativeMeta):
                return sql_select(model)
            columns_to_select = model.get_columns().keys()
        to_select = [getattr(model, col) for col in columns_to_select]
        return sql_select(*to_select)

    def asc(
        self, model: DeclarativeMeta, column: str, assert_model_colum: bool = True
    ) -> UnaryExpression:
        """Create an ascending order clause for the given column.

        Args:
            model (DeclarativeMeta): The model class to create an ascending order clause for. Use self.models.all_models() method to check valid models.
            column: The column to create an ascending order clause for.
            assert_model_colum (bool): Whether to assert the model and column validity. Defaults to True. Use this as true when calling the method directly if the model and columns haven't been asserted before. Methods get, add, delete, update and count already asserts the models and columns.

        Raises:
            AssertionError: If the provided model is not mapped or if the column is invalid.

        Returns:
            UnaryExpression: A SQLAlchemy ascending order clause for the given column.
        """
        if assert_model_colum:
            self._assert_model(model)
            self._assert_columns_in_model(model, [column])
        return sql_asc(getattr(model, column))

    def desc(
        self, model: DeclarativeMeta, column: str, assert_model_column: bool = True
    ) -> UnaryExpression:
        """Create a descending order clause for the given column.

        Args:
            model (DeclarativeMeta): The model class to create a descending order clause for. Use self.models.all_models() method to check valid models.
            column: The column to create a descending order clause for.
            assert_model_column (bool): Whether to assert the model and column validity. Defaults to True. Use this as true when calling the method directly if the model and columns haven't been asserted before. Methods get, add, delete, update and count already asserts the models and columns.

        Raises:
            AssertionError: If the provided model is not mapped or if the column is invalid.

        Returns:
            UnaryExpression: A SQLAlchemy descending order clause for the given column.
        """
        if assert_model_column:
            self._assert_model(model)
            self._assert_columns_in_model(model, [column])
        return sql_desc(getattr(model, column))

    def add(
        self, model: DeclarativeMeta, data: list[dict | DeclarativeMeta]
    ) -> dict[str, Any]:
        """add one or more entries for a model using dict (just the object, no relationships or other orm attributes), or the model instances instead (with all orm attributes, including relationships).

        Args:
            model (DeclarativeMeta): The model class to add instances of. Use self.models.all_models() method to check valid models.
            data (list[dict | DeclarativeMeta]): List of dictionaries with column-value mappings for bulk creation or the models instances. Use self.models.<model_name>.get_columns() to check valid columns.

        Returns:
            dict[str, Any]: A dictionary indicating success or failure. In case of failure, includes an error message.
        """
        self._assert_model(model)
        for i, item_data in enumerate(data):
            if isinstance(item_data, dict):
                self._assert_columns_in_model(
                    model,
                    list(item_data.keys()),
                    exclude_primary_key=True,
                    msg=" when adding new instances.",
                )
                data[i] = model(**item_data)
            elif isinstance(item_data, model):
                self._assert_columns_in_model(
                    item_data,
                    item_data.get_columns(),
                    exclude_primary_key=True,
                    msg=" when adding new instances.",
                )
            else:
                raise AssertionError(
                    f"The data in position {i} is invalid ({type(item_data).__name__}). It must be a dict or a {model.__name__}."
                )
        try:
            with self.get_session() as session:
                session.add_all(data)
                return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete(self, model: DeclarativeMeta, **query_kwargs) -> int:
        """Delete an entity, including it's children. To be implemented by subclasses.

        Args:
            model: The model class to delete from. Use self.models.all_models() method to check valid models.
            **query_kwargs: Filter criteria for deletion. Use self.models.<model_name>.get_columns() to check valid columns.

        Returns:
            int: The number of rows deleted.
        """
        self._assert_model(model)
        self._assert_columns_in_model(model, list(query_kwargs.keys()))
        stmt = delete(model)
        stmt = self.construct_where_clause(
            stmt, model, query_kwargs, assert_model_columns=False
        )
        with self.get_session() as session:
            result = session.execute(stmt)
            return result.rowcount

    def get(
        self,
        model,
        limit: int = None,
        columns_to_return: list[str] = None,
        columns_to_order_by: dict[str, bool] = None,
        **query_kwargs,
    ) -> list[dict]:
        """Find an entity. Doesn't support relationships.

        Args:
            model: The model class to search in. Use self.models.all_models() method to check valid models.
            limit (int, optional): Maximum number of results to return. Defaults to None (no limit).
            columns_to_return (list[str], optional): List of column names to return. Defaults to None (all columns).
            columns_to_order_by (dict[str, bool], optional): Dictionary of column names to order the results by and a boolean indicating ascending (True) or descending (False) order. Defaults to None (no specific order).
            **query_kwargs: Filter criteria for finding. Use self.models.<model_name>.get_columns() to check valid columns.

        Returns:
            list[dict]: List of found entity instances (dictionaries).
        """
        self._assert_model(model)
        self._assert_columns_in_model(model, list(query_kwargs.keys()))
        if columns_to_return:
            self._assert_columns_in_model(model, columns_to_return)
        if columns_to_order_by:
            self._assert_columns_in_model(model, list(columns_to_order_by.keys()))
        stmt = self.select(model, columns_to_return, assert_model=False)
        if not columns_to_return:
            columns_to_return = model.get_columns().keys()
        stmt = self.construct_where_clause(
            stmt, model, query_kwargs, assert_model_columns=False
        )
        if limit:
            stmt = stmt.limit(limit)
        if columns_to_order_by:
            order_by_columns = [
                self.asc(model, col, assert_model_colum=False)
                if asc_desc
                else self.desc(model, col, assert_model_column=False)
                for col, asc_desc in columns_to_order_by.items()
            ]
            stmt = stmt.order_by(*order_by_columns)
        with self.get_session() as session:
            result = session.execute(stmt)
            return [dict(row) for row in result.mappings().all()]

    def update(self, model, data_to_be_updated: dict, **query_kwargs) -> int:
        """Update entities matching criteria, Just the entry (doesn't support ORM related updates).

        Args:
            model: The model class to update. Use self.models.all_models() method to check valid models.
            data_to_be_updated (dict): Dictionary of column-value pairs to be updated.
            **kwargs: Update criteria and values. Use self.models.<model_name>.get_columns() to check valid columns.

        Returns:
            int: The number of rows updated.
        """
        self._assert_model(model)
        self._assert_columns_in_model(model, list(query_kwargs.keys()))
        self._assert_columns_in_model(
            model,
            list(data_to_be_updated.keys()),
            exclude_primary_key=True,
            msg=" in the updated values when updating instances.",
        )
        stmt = update(model)
        data_to_be_updated["updated_at"] = utils.utc_now()
        stmt = self.construct_where_clause(
            stmt, model, query_kwargs, assert_model_columns=False
        )
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
        self._assert_columns_in_model(model, list(query_kwargs.keys()))
        stmt = self.select(func.count(), assert_model=False).select_from(model)
        stmt = self.construct_where_clause(
            stmt, model, query_kwargs, assert_model_columns=False
        )
        with self.get_session() as session:
            result = session.execute(stmt).scalar()
            return result
