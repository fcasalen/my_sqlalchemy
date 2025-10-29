"""
count_assertions test are there to test if the methods select, get, add, delete, update and count don't overvalidate
"""

from unittest.mock import Mock, patch

import pytest
import re
from sqlalchemy import Column, DateTime, Integer, String, select, UUID
from sqlalchemy.orm import Session, declarative_base, DeclarativeMeta
import uuid
from unittest.mock import call

from src.my_sqlalchemy.my_sqlalchemy import (
    MySQLAlchemy,
    InstrumentedAttribute,
    UnaryExpression,
)
from src.my_sqlalchemy.standard_model import StandardModel

Base = declarative_base()

invalid_base = declarative_base()


class MockModel(StandardModel):
    __tablename__ = "test_table"
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    uuid = Column(UUID, nullable=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class InvalidModel(invalid_base):
    __tablename__ = "invalid_table"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))


class NotaModel:
    pass


@pytest.fixture
def mysql_alchemy():
    my_sql = MySQLAlchemy("sqlite:///:memory:")
    with my_sql.get_session() as session:
        session.add_all(
            [MockModel(**d) for d in [{"name": "test1"}, {"name": "test2"}]]
        )
        session.flush()
    yield my_sql
    my_sql.engine.dispose()


@pytest.fixture
def mock_session():
    session = Mock()
    session.commit = Mock()
    session.rollback = Mock()
    session.close = Mock()
    session.add_all = Mock()
    session.flush = Mock()
    session.execute = Mock()
    session.expunge = Mock()
    return session


@pytest.fixture
def mock_asserter():
    with patch("src.my_sqlalchemy.my_sqlalchemy.asserter") as mock_asserter:
        mock_asserter.model.return_value = None
        mock_asserter.primary_key_no_values.return_value = None
        mock_asserter.columns_same_model.return_value = None
        mock_asserter.columns_values_are_same_type.return_value = None
        mock_asserter.conditions.return_value = None
        mock_asserter.list_of.return_value = None
        yield mock_asserter


def count_assertions(
    mocked_asserter,
    model_call_args_list: list = [],
    columns_same_model_call_args_list: list = [],
    columns_values_are_same_type_arg_list: list = [],
    conditions_call_args_list: list = [],
    list_of_call_args_list: list = [],
    primary_key_no_values_call_args_list: list = [],
):
    """Assert that the mocked asserter methods were called the expected number of times with the expected arguments.

    Args:
        mocked_asserter (_type_): _description_
        model_call_args_list (list, optional): _description_. Defaults to [].
        columns_same_model_call_args_list (list, optional): _description_. Defaults to [].
        columns_values_are_same_type_arg_list (list, optional): _description_. Defaults to [].
        conditions_call_args_list (list, optional): _description_. Defaults to [].
        list_of_call_args_list (list, optional): _description_. Defaults to [].
        primary_key_no_values_call_args_list (list, optional): _description_. Defaults to []. Use None to skip this check.
    """
    assert mocked_asserter.model.call_args_list == model_call_args_list, (
        f"model call {mocked_asserter.model.call_args_list} did not match expected calls {model_call_args_list}."
    )
    assert (
        mocked_asserter.columns_same_model.call_args_list
        == columns_same_model_call_args_list
    ), (
        f"columns_same_model call {mocked_asserter.columns_same_model.call_args_list} did not match expected calls {columns_same_model_call_args_list}."
    )
    assert (
        mocked_asserter.columns_values_are_same_type.call_args_list
        == columns_values_are_same_type_arg_list
    ), (
        f"columns_values_are_same_type call {mocked_asserter.columns_values_are_same_type.call_args_list} did not match expected calls {columns_values_are_same_type_arg_list}."
    )
    assert mocked_asserter.conditions.call_args_list == conditions_call_args_list, (
        f"conditions call {mocked_asserter.conditions.call_args_list} did not match expected calls {conditions_call_args_list}."
    )
    assert mocked_asserter.list_of.call_args_list == list_of_call_args_list, (
        f"list_of call {mocked_asserter.list_of.call_args_list} did not match expected calls {list_of_call_args_list}."
    )
    if primary_key_no_values_call_args_list is not None:
        assert (
            mocked_asserter.primary_key_no_values.call_args_list
            == primary_key_no_values_call_args_list
        ), (
            f"primary_key_no_values call {mocked_asserter.primary_key_no_values.call_args_list} did not match expected calls {primary_key_no_values_call_args_list}."
        )


class TestGeneral:
    def test_init(self):
        db = MySQLAlchemy("sqlite:///:memory:")
        assert db.database_url == "sqlite:///:memory:"
        assert db.engine is not None
        assert db.SessionLocal is not None
        db.engine.dispose()


class TestGetSession:
    def test_get_session_success(
        self, mysql_alchemy: MySQLAlchemy, mock_session: Session
    ):
        with patch.object(mysql_alchemy, "SessionLocal", return_value=mock_session):
            with mysql_alchemy.get_session():
                pass
            mock_session.commit.assert_called_once()
            mock_session.close.assert_called_once()

    def test_get_session_exception(
        self, mysql_alchemy: MySQLAlchemy, mock_session: Session
    ):
        mock_session.commit.side_effect = Exception("Test error")
        with patch.object(mysql_alchemy, "SessionLocal", return_value=mock_session):
            with pytest.raises(Exception):
                with mysql_alchemy.get_session():
                    pass
            mock_session.rollback.assert_called_once()
            mock_session.close.assert_called_once()


class TestSelect:
    def test_select_single_model(self, mysql_alchemy: MySQLAlchemy):
        stmt = mysql_alchemy.select(MockModel)
        expected_stmt = select(MockModel)
        assert str(stmt) == str(expected_stmt)

    def test_select_multiple_columns(self, mysql_alchemy: MySQLAlchemy):
        stmt = mysql_alchemy.select([MockModel.name, MockModel.id])
        expected_stmt = select(MockModel.name, MockModel.id)
        assert str(stmt) == str(expected_stmt)

    def test_select_different_models(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                "The following columns {'InvalidModel.name'} do not belong to the model MockModel."
            ),
        ):
            mysql_alchemy.select([MockModel.name, InvalidModel.name])

    def test_select_invalid_list(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            TypeError,
            match=re.escape(
                "The following items ['0 - (invalid_data)'] are not a list of type InstrumentedAttribute."
            ),
        ):
            mysql_alchemy.select(["invalid_data"])

    def test_select_valid_model_count_assertions(
        self, mysql_alchemy: MySQLAlchemy, mock_asserter
    ):
        mysql_alchemy.select(MockModel)
        count_assertions(mock_asserter, [call(mysql_alchemy.base.metadata, MockModel)])

    def test_select_valid_columns_count_assertions(
        self, mysql_alchemy: MySQLAlchemy, mock_asserter
    ):
        mysql_alchemy.select([MockModel.name, MockModel.id])
        count_assertions(
            mock_asserter,
            list_of_call_args_list=[
                call([MockModel.name, MockModel.id], InstrumentedAttribute)
            ],
            columns_same_model_call_args_list=[
                call(MockModel, [MockModel.name, MockModel.id])
            ],
        )


class TestGet:
    def test_get_invalid_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                "The models passed (position, name) ['0 - (InvalidModel)'] are not mapped in the database."
            ),
        ):
            mysql_alchemy.get(InvalidModel)

    def test_not_a_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                "The models passed (position, name) ['0 - (NotaModel)'] are not mapped in the database."
            ),
        ):
            mysql_alchemy.get(NotaModel)

    def test_get_simple(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(MockModel, convert_results_to_dictionaries=True)
        assert results == [
            {
                "id": 1,
                "name": "test1",
                "created_at": None,
                "updated_at": None,
                "uuid": None,
            },
            {
                "id": 2,
                "name": "test2",
                "created_at": None,
                "updated_at": None,
                "uuid": None,
            },
        ]
        results = mysql_alchemy.get(MockModel)
        assert isinstance(results[0], MockModel)
        assert isinstance(results[1], MockModel)

    def test_get_model_count_assertions(
        self, mysql_alchemy: MySQLAlchemy, mock_asserter
    ):
        conditions = [MockModel.id == 0]
        columns_to_order_by = [MockModel.name.asc()]
        mysql_alchemy.get(
            MockModel, columns_to_order_by=columns_to_order_by, conditions=conditions
        )
        count_assertions(
            mock_asserter,
            model_call_args_list=[call(mysql_alchemy.base.metadata, MockModel)],
            conditions_call_args_list=[call(MockModel, conditions)],
            columns_same_model_call_args_list=[call(MockModel, [MockModel.name])],
            list_of_call_args_list=[call(columns_to_order_by, UnaryExpression)],
        )

    def test_get_model_columns_count_assertions(
        self, mysql_alchemy: MySQLAlchemy, mock_asserter
    ):
        conditions = [MockModel.id == 0]
        columns_to_order_by = [MockModel.name.asc()]
        selection = [MockModel.name, MockModel.id]
        mysql_alchemy.get(
            selection=selection,
            columns_to_order_by=columns_to_order_by,
            conditions=conditions,
        )
        count_assertions(
            mock_asserter,
            conditions_call_args_list=[call(MockModel, conditions)],
            columns_same_model_call_args_list=[
                call(MockModel, selection),
                call(MockModel, [MockModel.name]),
            ],
            list_of_call_args_list=[
                call(selection, InstrumentedAttribute),
                call(columns_to_order_by, UnaryExpression),
            ],
        )

    def test_get_simple_ordered_by_asc(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(
            MockModel,
            columns_to_order_by=[MockModel.name.asc()],
            convert_results_to_dictionaries=True,
        )
        assert results == [
            {
                "id": 1,
                "name": "test1",
                "created_at": None,
                "updated_at": None,
                "uuid": None,
            },
            {
                "id": 2,
                "name": "test2",
                "created_at": None,
                "updated_at": None,
                "uuid": None,
            },
        ]

    def test_get_simple_ordered_by_desc(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(
            MockModel,
            columns_to_order_by=[MockModel.name.desc()],
            convert_results_to_dictionaries=True,
        )
        assert results == [
            {
                "id": 2,
                "name": "test2",
                "created_at": None,
                "updated_at": None,
                "uuid": None,
            },
            {
                "id": 1,
                "name": "test1",
                "created_at": None,
                "updated_at": None,
                "uuid": None,
            },
        ]

    def test_get_by_columns(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(
            MockModel,
            conditions=[MockModel.name == "test1"],
            convert_results_to_dictionaries=True,
        )
        assert results == [
            {
                "id": 1,
                "name": "test1",
                "created_at": None,
                "updated_at": None,
                "uuid": None,
            }
        ]

    def test_get_return_only_a_column(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(
            [MockModel.name], convert_results_to_dictionaries=True
        )
        assert results == [{"name": "test1"}, {"name": "test2"}]

    def test_get_return_only_two_columns(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(
            [MockModel.name, MockModel.id], convert_results_to_dictionaries=True
        )
        assert results == [{"name": "test1", "id": 1}, {"name": "test2", "id": 2}]

    def test_get_with_limit(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(
            MockModel, limit=1, convert_results_to_dictionaries=True
        )
        assert results == [
            {
                "id": 1,
                "name": "test1",
                "created_at": None,
                "updated_at": None,
                "uuid": None,
            }
        ]


class TestAdd:
    def test_add_invalid_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            TypeError,
            match=re.escape(
                "The following items ['0 - (InvalidModel)'] are not a list of type DeclarativeMeta."
            ),
        ):
            mysql_alchemy.add([InvalidModel(**{"name": "test3"})])

    def test_add(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.add(
            [MockModel(**{"name": "test3"}), MockModel(**{"name": "test4"})]
        ) == {"success": True}
        with mysql_alchemy.SessionLocal() as session:
            results = session.scalars(select(MockModel)).all()
            results = mysql_alchemy.results_to_dictionaries(results)
            assert results == [
                {
                    "created_at": None,
                    "id": 1,
                    "name": "test1",
                    "updated_at": None,
                    "uuid": None,
                },
                {
                    "created_at": None,
                    "id": 2,
                    "name": "test2",
                    "updated_at": None,
                    "uuid": None,
                },
                {
                    "created_at": None,
                    "id": 3,
                    "name": "test3",
                    "updated_at": None,
                    "uuid": None,
                },
                {
                    "created_at": None,
                    "id": 4,
                    "name": "test4",
                    "updated_at": None,
                    "uuid": None,
                },
            ]

    def test_add_count_assertions(self, mysql_alchemy: MySQLAlchemy, mock_asserter):
        new_instance = MockModel(name="test3")
        with patch.object(mysql_alchemy, "get_session"):
            mysql_alchemy.add([new_instance])
        count_assertions(
            mock_asserter,
            list_of_call_args_list=[
                call([new_instance], DeclarativeMeta, mysql_alchemy.base.metadata)
            ],
            primary_key_no_values_call_args_list=[
                call(new_instance, msg=" in the instance 0 to be added.")
            ],
        )

    def test_add_session_error(self, mysql_alchemy: MySQLAlchemy):
        with patch.object(mysql_alchemy, "get_session") as mock_get_session:
            mock_session = Mock()
            mock_session.add_all.side_effect = Exception("Add error")
            mock_get_session.return_value.__enter__.return_value = mock_session
            assert mysql_alchemy.add([MockModel(**{"name": "test3"})]) == {
                "success": False,
                "error": "Add error",
            }

    def test_add_model_instances_invalid_primary_key(self, mysql_alchemy: MySQLAlchemy):
        new_instance = MockModel(id=1, name="test3")
        with pytest.raises(
            AssertionError,
            match="The following primary key columns {'id'} should not have values in the instance 0 to be added.",
        ):
            mysql_alchemy.add([new_instance])


class TestUpdate:
    def test_update_invalid_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                "The models passed (position, name) ['0 - (InvalidModel)'] are not mapped in the database."
            ),
        ):
            mysql_alchemy.update(
                [(InvalidModel.name, "test1_updated")],
                conditions=[InvalidModel.name == "test1"],
            )

    def test_update_with_query(self, mysql_alchemy: MySQLAlchemy):
        updated_count = mysql_alchemy.update(
            [(MockModel.name, "test1_updated"), (MockModel.uuid, uuid.uuid4())],
            [MockModel.id == 1],
        )
        assert updated_count == 1
        with mysql_alchemy.SessionLocal() as session:
            results = session.scalars(select(MockModel)).all()
            results = [
                {k: v for k, v in result.__dict__.items() if k != "_sa_instance_state"}
                for result in results
            ]
            assert results == [
                {
                    "created_at": None,
                    "id": 1,
                    "name": "test1_updated",
                    "updated_at": results[0]["updated_at"],
                    "uuid": results[0]["uuid"],
                },
                {
                    "created_at": None,
                    "id": 2,
                    "name": "test2",
                    "updated_at": None,
                    "uuid": None,
                },
            ]

    def test_update_all(self, mysql_alchemy: MySQLAlchemy):
        updated_count = mysql_alchemy.update(
            [(MockModel.name, "test1_updated")], [MockModel.id > 0]
        )
        assert updated_count == 2
        with mysql_alchemy.SessionLocal() as session:
            results = session.scalars(select(MockModel)).all()
            results = mysql_alchemy.results_to_dictionaries(results)
            assert results == [
                {
                    "created_at": None,
                    "id": 1,
                    "name": "test1_updated",
                    "updated_at": results[0]["updated_at"],
                    "uuid": None,
                },
                {
                    "created_at": None,
                    "id": 2,
                    "name": "test1_updated",
                    "updated_at": results[1]["updated_at"],
                    "uuid": None,
                },
            ]

    def test_update_invalid_none_value(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            TypeError,
            match=re.escape("Column 'name' does not accept None values."),
        ):
            mysql_alchemy.update([(MockModel.name, None)], [MockModel.id == 1])

    def test_update_invalid_type_value(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            TypeError,
            match=re.escape(
                "Column 'id' expects values of type 'int', but got value 'invalid_int' of type 'str'."
            ),
        ):
            mysql_alchemy.update(
                [(MockModel.id, "invalid_int")], [MockModel.name == "test1"]
            )

    def test_update_query_no_values(self, mysql_alchemy: MySQLAlchemy):
        updated_count = mysql_alchemy.update(
            [(MockModel.name, "test1_updated")], [MockModel.id == 100]
        )
        assert updated_count == 0
        with mysql_alchemy.SessionLocal() as session:
            results = session.scalars(select(MockModel)).all()
            results = mysql_alchemy.results_to_dictionaries(results)
            assert results == [
                {
                    "created_at": None,
                    "id": 1,
                    "name": "test1",
                    "updated_at": None,
                    "uuid": None,
                },
                {
                    "created_at": None,
                    "id": 2,
                    "name": "test2",
                    "updated_at": None,
                    "uuid": None,
                },
            ]

    def test_update_count_assertions(self, mysql_alchemy: MySQLAlchemy, mock_asserter):
        conditions = [MockModel.id == 1]
        update_values = [(MockModel.name, "test1_updated")]
        mysql_alchemy.update(update_values, conditions=conditions)
        columns = [col for col, val in update_values]
        values = [val for col, val in update_values]
        count_assertions(
            mock_asserter,
            model_call_args_list=[call(mysql_alchemy.base.metadata, MockModel)],
            columns_same_model_call_args_list=[call(MockModel, columns)],
            columns_values_are_same_type_arg_list=[call(columns, values)],
            conditions_call_args_list=[call(MockModel, conditions)],
            list_of_call_args_list=[
                call(update_values, tuple),
                call(columns, InstrumentedAttribute),
            ],
            primary_key_no_values_call_args_list=None,
        )
        primary_key_call = mock_asserter.primary_key_no_values.call_args_list
        assert len(primary_key_call) == 1
        assert mysql_alchemy.results_to_dictionaries([primary_key_call[0][0][0]]) == [
            {"name": "test1_updated"}
        ]


class TestCount:
    def test_count_invalid_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                "The models passed (position, name) ['0 - (InvalidModel)'] are not mapped in the database."
            ),
        ):
            mysql_alchemy.count(InvalidModel)

    def test_count_all(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.count(MockModel) == 2

    def test_count_query(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.count(MockModel, [MockModel.name == "test1"]) == 1

    def test_count_query_no_values(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.count(MockModel, [MockModel.name == "test10"]) == 0


class TestDelete:
    def test_delete_invalid_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                "The models passed (position, name) ['0 - (InvalidModel)'] are not mapped in the database."
            ),
        ):
            mysql_alchemy.delete(InvalidModel, [InvalidModel.name == "test1"])

    def test_delete_all(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.delete(MockModel, [MockModel.id > 0]) == 2

    def test_delete_with_query(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.delete(MockModel, [MockModel.name == "test1"]) == 1

    def test_delete_query_no_values(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.delete(MockModel, [MockModel.name == "non_existent"]) == 0

    def test_delete_count_assertions(self, mysql_alchemy: MySQLAlchemy, mock_asserter):
        conditions = [MockModel.id == 1]
        mysql_alchemy.delete(MockModel, conditions=conditions)
        count_assertions(
            mock_asserter,
            model_call_args_list=[call(mysql_alchemy.base.metadata, MockModel)],
            conditions_call_args_list=[call(MockModel, conditions)],
        )
