"""Methods to assert various conditions on SQLAlchemy models and their attributes before performing database operations."""

from sqlalchemy import inspect, BinaryExpression, MetaData
from sqlalchemy.orm import DeclarativeMeta, InstrumentedAttribute
from typing import Any


def model(
    base_metadata: MetaData, models: DeclarativeMeta | list[DeclarativeMeta]
) -> None:
    """Assert that the provided models are valid DeclarativeMeta.

    Args:
        base_metadata (MetaData): The metadata of the base to validate models against.
        model (DeclarativeMeta | list[DeclarativeMeta]): The model class to validate

    Raises:
        AssertionError: If the provided model is not mapped.
    """
    if not isinstance(models, list):
        models = [models]
    errors = []
    for i, m in enumerate(models):
        try:
            if inspect(m).mapper.local_table.metadata != base_metadata:
                errors.append(f"{i} - ({m.__name__})")
        except Exception:
            errors.append(f"{i} - ({m.__name__})")
    if errors:
        raise AssertionError(
            f"The models passed (position, name) {errors} are not mapped in the database."
        )


def columns_same_model(
    model: DeclarativeMeta, columns: list[InstrumentedAttribute], title: str = ""
) -> None:
    """Assert that the provided columns belong to the given model.

    Args:
        model (DeclarativeMeta): The model class to validate columns against.
        columns (list[InstrumentedAttribute]): List of columns to validate.
        title (str, optional): Title for the assertion error message. Defaults to "".

    Raises:
        AssertionError: If any of the provided columns do not belong to the given model.
    """
    errors = []
    mapped_keys = list(inspect(model).mapper.column_attrs)
    for col in columns:
        if col not in mapped_keys:
            errors.append(f"{col.class_.__name__}.{col.key}")
    assert not errors, (
        f"{title}The following columns {set(errors)} do not belong to the model {model.__name__}."
    )


def primary_key_no_values(model_instance: DeclarativeMeta, msg: str = "") -> None:
    """Assert that the primary key columns of the provided model instance have no values.

    Args:
        model_instance (DeclarativeMeta): The model instance to validate.
        msg (str, optional): Additional message for the assertion error. Defaults to "".

    Raises:
        AssertionError: If any of the primary key columns have values.
    """
    errors = []
    primary_keys = [key.name for key in model_instance.__table__.primary_key.columns]
    for pk in primary_keys:
        if getattr(model_instance, pk) is not None:
            errors.append(pk)
    assert not errors, (
        f"The following primary key columns {set(errors)} should not have values{msg}."
    )


def columns_values_are_same_type(
    columns: list[InstrumentedAttribute], values: list[Any], title: str = ""
) -> None:
    """Assert that the provided columns and values are of the same type.

    Args:
        columns (list[InstrumentedAttribute]): List of columns to validate.
        values (list[Any]): List of values to validate.
        title (str, optional): Title for the assertion error message. Defaults to "".

    Raises:
        AssertionError: If any of the provided columns and values are not of the same type.
    """
    errors = []
    for column, value in zip(columns, values):
        column_type = column.type
        expected_python_type = column_type.python_type
        if value is None and not column.nullable:
            errors.append(f"Column '{column.key}' does not accept None values.")
            continue
        if not isinstance(value, expected_python_type):
            errors.append(
                f"Column '{column.key}' expects values of type '{expected_python_type.__name__}', "
                f"but got value '{value}' of type '{type(value).__name__}'."
            )
    if errors:
        raise TypeError(f"{title}{' | '.join(errors)}")


def filter(model: DeclarativeMeta, filter: list[BinaryExpression]) -> None:
    """Assert that the provided filter belong to the given model and columns values are of the same type.

    Args:
        model (DeclarativeMeta): The model class to validate filter against.
        filter (list[BinaryExpression]): List of filter to validate.

    Raises:
        AssertionError: If any of the provided filter do not belong to the given model.
    """
    list_of(filter, BinaryExpression, title="Checking filter: ")
    columns = [condition.left for condition in filter]
    columns_same_model(model, columns, title="Checking filter: ")
    columns_values_are_same_type(
        columns,
        [condition.right.effective_value for condition in filter],
        title="Checking filter: ",
    )


def list_of(
    data: list[Any], type_: Any, base_metadata: MetaData = None, title: str = ""
) -> None:
    """Assert that the provided data is a list of items of the specified type.

    Args:
        data (list[Any]): The data to validate.
        type_ (Any): The expected type of the items in the list.
        base_metadata (MetaData, optional): The base metadata to validate models against. Required if type_ is DeclarativeMeta.
        title (str, optional): Title for the assertion error message. Defaults to "".

    Raises:
        TypeError: If the provided data is not a list or if the items are not of the specified type.
    """
    assert isinstance(data, list), "The provided data should be a list."
    errors = []
    if type_ == DeclarativeMeta:
        if base_metadata is None:
            raise ValueError(
                "base_metadata must be provided when type_ is DeclarativeMeta."
            )
        for i, item in enumerate(data):
            try:
                if inspect(item).mapper.local_table.metadata != base_metadata:
                    errors.append(f"{i} - ({item.__name__})")
            except Exception:
                errors.append(f"{i} - ({item.__class__.__name__})")
    else:
        for i, item in enumerate(data):
            if not isinstance(item, type_):
                errors.append(f"{i} - ({item})")
    if errors:
        raise TypeError(
            f"{title}The following items {errors} are not a list of type {type_.__name__}."
        )
