from contextlib import contextmanager
from typing import Any

from sqlalchemy import (
    create_engine,
    delete,
    func,
    select as sql_select,
    update,
    UnaryExpression,
    BinaryExpression,
    Select,
)
from sqlalchemy.orm import sessionmaker, InstrumentedAttribute, make_transient
from sqlalchemy.orm import DeclarativeMeta

from .base import Base
from . import asserter


class MySQLAlchemy:
    """A simple sqlalchemy wrapper"""

    def __init__(self, database_url: str, base: DeclarativeMeta = Base):
        """Initialize the service with database connection.

        Args:
            database_url (str): The database connection URL.
            base (DeclarativeMeta, optional): The declarative base containing the models. Defaults to Base, which has a model StandardModel with id (primary key), created_at and updated_at with default values as UTC now (it captures the datetime when the model object is instantiated).
        """
        self.database_url = database_url
        self.base = base
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

    def results_to_dictionaries(
        self, results: list[DeclarativeMeta]
    ) -> list[dict[str, Any]]:
        """Convert a list of SQLAlchemy model instances to a list of dictionaries.

        Args:
            results (list[DeclarativeMeta]): List of SQLAlchemy model instances.

        Returns:
            list[dict[str, Any]]: List of dictionaries representing the model instances.
        """
        list_of_dicts = [
            {k: v for k, v in result.__dict__.items() if k != "_sa_instance_state"}
            for result in results
        ]
        return list_of_dicts

    def select(
        self, selection: DeclarativeMeta | list[InstrumentedAttribute]
    ) -> Select:
        """Create a select statement for the given model (one model only) or list of model columns (they should be all from the same model).

        Args:
            selection (DeclarativeMeta | list[InstrumentedAttribute]): The model class or list of columns to create a select statement for.

        Raises:
            AssertionError: If the provided model is not mapped or if the columns are from different models.

        Returns:
            Select: A SQLAlchemy select statement for the given model.
        """
        if isinstance(selection, list):
            asserter.list_of(selection, InstrumentedAttribute)
            asserter.columns_same_model(selection[0].class_, selection)
            return sql_select(*selection)
        asserter.model(self.base.metadata, selection)
        return sql_select(selection)

    def add(self, data: list[DeclarativeMeta]) -> dict[str, str | bool]:
        """add model instances (with all orm attributes, including relationships).

        Args:
            data (list[DeclarativeMeta]): List of the models instances.

        Returns:
            dict[str, str | bool]: A dictionary indicating success or failure. In case of failure, includes an error message.
        """
        asserter.list_of(data, DeclarativeMeta, self.base.metadata)
        for i, model_instance in enumerate(data):
            asserter.primary_key_no_values(
                model_instance, msg=f" in the instance {i} to be added."
            )
        try:
            with self.get_session() as session:
                session.add_all(data)
                return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete(self, model: DeclarativeMeta, filter: list[BinaryExpression]) -> int:
        """Delete an entity, including it's children. To be implemented by subclasses.

        Args:
            model (DeclarativeMeta): The model class to delete from.
            filter (list[BinaryExpression]): Conditions to filter which rows to delete.

        Returns:
            int: The number of rows deleted.
        """
        asserter.model(self.base.metadata, model)
        stmt = delete(model)
        asserter.filter(model, filter)
        stmt = stmt.where(*filter)
        with self.get_session() as session:
            result = session.execute(stmt)
            return result.rowcount

    def get(
        self,
        selection: DeclarativeMeta | list[InstrumentedAttribute],
        limit: int = None,
        order_by: list[UnaryExpression] = None,
        filter: list[BinaryExpression] = None,
        convert_results_to_dictionaries: bool = False,
    ) -> list[DeclarativeMeta] | list[dict[str, Any]]:
        """Find an entity. Doesn't support relationships.

        Args:
            selection (DeclarativeMeta | list[InstrumentedAttribute]): The model class or list of columns to select from.
            limit (int, optional): Maximum number of results to return. Defaults to None (no limit).
            order_by (list[UnaryExpression], optional): List of columns to order the results by. Each item should be a tuple of (column, asc_desc) where asc_desc is a boolean indicating ascending (True) or descending (False) order. Defaults to None.
            filter (list[BinaryExpression], optional): Conditions to filter which rows to retrieve. Defaults to None.
            convert_results_to_dictionaries (bool, optional): Whether to convert results to list of dictionaries. Defaults to False.

        Returns:
            list[DeclarativeMeta] | list[dict[str, Any]]: List of model instances or list of dictionaries representing the model instances.
        """
        if isinstance(selection, list):
            model = selection[0].class_
            pk_columns = [
                getattr(model, pk_col.key)
                for pk_col in model.__table__.primary_key.columns
            ]
            selection = list(set(selection).union(set(pk_columns)))
        else:
            model = selection
        stmt = self.select(selection)
        if filter:
            asserter.filter(model, filter)
            stmt = stmt.where(*filter)
        if limit:
            assert isinstance(limit, int) and limit > 0, (
                "Limit should be a positive integer."
            )
            stmt = stmt.limit(limit)
        if order_by:
            asserter.list_of(order_by, UnaryExpression)
            asserter.columns_same_model(model, [col.element for col in order_by])
            stmt = stmt.order_by(*order_by)
        with self.get_session() as session:
            if model == selection:
                results = session.scalars(stmt).all()
                for result in results:
                    session.expunge(result)
                instances = results
            else:
                result = session.execute(stmt)
                list_of_dicts = [dict(row) for row in result.mappings().all()]
                instances = []
                for data_dict in list_of_dicts:
                    instance = model(**data_dict)
                    make_transient(instance)  # Optional, but explicit
                    instances.append(instance)
            if convert_results_to_dictionaries:
                return self.results_to_dictionaries(instances)
            return instances

    def update(
        self,
        data_to_be_updated: list[tuple[InstrumentedAttribute, Any]],
        filter: list[BinaryExpression],
    ) -> int:
        """Update entities matching criteria, Just the entry (doesn't support ORM related updates).

        Args:
            data_to_be_updated (list[tuple[InstrumentedAttribute, Any]]): List of tuples where each tuple contains a column (from the same model) and the new value to set.
            filter (list[BinaryExpression]): Conditions to filter which rows to update.

        Returns:
            int: The number of rows updated.
        """
        asserter.list_of(data_to_be_updated, tuple)
        columns = [t[0] for t in data_to_be_updated]
        values = [t[1] for t in data_to_be_updated]
        asserter.list_of(columns, InstrumentedAttribute)
        model = columns[0].class_
        asserter.model(self.base.metadata, model)
        asserter.columns_same_model(model, columns)
        asserter.columns_values_are_same_type(columns, values)
        model_instance = model(**{col.key: value for col, value in data_to_be_updated})
        asserter.primary_key_no_values(model_instance)
        stmt = update(model)
        if filter:
            asserter.filter(model, filter)
            stmt = stmt.where(*filter)
        update_data_dict = {column.key: value for column, value in data_to_be_updated}
        stmt = stmt.values(**update_data_dict)
        with self.get_session() as session:
            result = session.execute(stmt)
            return result.rowcount

    def count(
        self, model: DeclarativeMeta, filter: list[BinaryExpression] = None
    ) -> int:
        """Count entities matching criteria.

        Args:
            model (DeclarativeMeta): The model class to count from.
            filter (list[BinaryExpression]): Conditions to filter which rows to count.

        Returns:
            int: The count of matching entities.
        """
        asserter.model(self.base.metadata, model)
        stmt = sql_select(func.count()).select_from(model)
        if filter:
            asserter.filter(model, filter)
            stmt = stmt.where(*filter)
        with self.get_session() as session:
            result = session.execute(stmt).scalar()
            return result
