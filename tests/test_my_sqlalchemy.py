from unittest.mock import Mock, patch

import pytest
import re
from sqlalchemy import Column, DateTime, Integer, String, select
from sqlalchemy.orm import Session, declarative_base

from src.my_sqlalchemy.models_handler import ModelsHandler
from src.my_sqlalchemy.my_sqlalchemy import MySQLAlchemy
from src.my_sqlalchemy.standard_model import StandardModel

Base = declarative_base()


class MockModel(StandardModel):
    __tablename__ = "test_table"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    @classmethod
    def columns(cls):
        return ["id", "name", "created_at", "updated_at"]


class InvalidModel:
    pass


@pytest.fixture
def test_models():
    return [MockModel]


@pytest.fixture
def mysql_alchemy(test_models):
    my_sql = MySQLAlchemy("sqlite:///:memory:", test_models)
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


class TestGeneral:
    def test_init(self, test_models):
        db = MySQLAlchemy("sqlite:///:memory:", test_models)
        assert db.database_url == "sqlite:///:memory:"
        assert isinstance(db.models, ModelsHandler)
        assert db.engine is not None
        assert db.SessionLocal is not None
        db.engine.dispose()

    def test_assert_model_valid(self, mysql_alchemy: MySQLAlchemy):
        mysql_alchemy._assert_model(MockModel)

    def test_assert_model_invalid(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                f"The model {InvalidModel} is not mapped. Valid models are: {['MockModel']}.\n\n"
                "Use self.models.all_models() to check valid models."
            ),
        ):
            mysql_alchemy._assert_model(InvalidModel)

    def test_assert_columns_in_model(self, mysql_alchemy: MySQLAlchemy):
        mysql_alchemy._assert_columns_in_model(MockModel, ["name"])
        with pytest.raises(
            AssertionError,
            match=re.escape(
                "Primary key column 'id' cannot be included in this operation."
            ),
        ):
            mysql_alchemy._assert_columns_in_model(
                MockModel,
                ["id", "name"],
                exclude_primary_key=True,
                msg=" in this operation.",
            )

    def test_select(self, mysql_alchemy: MySQLAlchemy):
        stmt = mysql_alchemy.select(MockModel)
        assert str(stmt) == str(select(MockModel))

        stmt = mysql_alchemy.select(MockModel, columns_to_select=["name", "id"])
        expected_stmt = select(getattr(MockModel, "name"), getattr(MockModel, "id"))
        assert str(stmt) == str(expected_stmt)

    def test_asc(self, mysql_alchemy: MySQLAlchemy):
        asc_expr = mysql_alchemy.asc(MockModel, "name")
        expected_expr = getattr(MockModel, "name").asc()
        assert str(asc_expr) == str(expected_expr)

    def test_desc(self, mysql_alchemy: MySQLAlchemy):
        desc_expr = mysql_alchemy.desc(MockModel, "name")
        expected_expr = getattr(MockModel, "name").desc()
        assert str(desc_expr) == str(expected_expr)


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


class TestGet:
    def test_get_invalid_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                f"The model {InvalidModel} is not mapped. Valid models are: {['MockModel']}.\n\n"
                "Use self.models.all_models() to check valid models."
            ),
        ):
            mysql_alchemy.get(InvalidModel)

    def test_get_simple(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(MockModel)
        assert results == [
            {"id": 1, "name": "test1", "created_at": None, "updated_at": None},
            {"id": 2, "name": "test2", "created_at": None, "updated_at": None},
        ]

    def test_get_simple_ordered_by_asc(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(MockModel, columns_to_order_by={"name": True})
        assert results == [
            {"id": 1, "name": "test1", "created_at": None, "updated_at": None},
            {"id": 2, "name": "test2", "created_at": None, "updated_at": None},
        ]

    def test_get_simple_ordered_by_desc(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(MockModel, columns_to_order_by={"name": False})
        assert results == [
            {"id": 2, "name": "test2", "created_at": None, "updated_at": None},
            {"id": 1, "name": "test1", "created_at": None, "updated_at": None},
        ]

    def test_get_by_columns(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(MockModel, name="test1")
        assert results == [
            {"id": 1, "name": "test1", "created_at": None, "updated_at": None}
        ]

    def test_get_return_only_a_column(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(MockModel, columns_to_return=["name"])
        assert results == [{"name": "test1"}, {"name": "test2"}]

    def test_get_return_only_two_columns(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(MockModel, columns_to_return=["name", "id"])
        assert results == [{"name": "test1", "id": 1}, {"name": "test2", "id": 2}]

    def test_get_with_limit(self, mysql_alchemy: MySQLAlchemy):
        results = mysql_alchemy.get(MockModel, limit=1)
        assert results == [
            {"id": 1, "name": "test1", "created_at": None, "updated_at": None}
        ]


class TestConstructWhereClause:
    def testconstruct_where_clause(self, mysql_alchemy):
        stmt = select(MockModel)
        query_kwargs = {"name": "test", "id": 1}

        result = mysql_alchemy.construct_where_clause(stmt, MockModel, query_kwargs)

        assert result is not None


class TestAdd:
    def test_add_invalid_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                f"The model {InvalidModel} is not mapped. Valid models are: {['MockModel']}.\n\n"
                "Use self.models.all_models() to check valid models."
            ),
        ):
            mysql_alchemy.add(InvalidModel, [{"name": "test3"}])

    def test_add(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.add(MockModel, [{"name": "test3"}, {"name": "test4"}]) == {
            "success": True
        }
        columns_to_return = [
            getattr(MockModel, column_name) for column_name in MockModel.columns()
        ]
        with mysql_alchemy.SessionLocal() as session:
            results = session.execute(select(*columns_to_return)).all()
            assert [dict(zip(MockModel.columns(), row)) for row in results] == [
                {
                    "created_at": None,
                    "id": 1,
                    "name": "test1",
                    "updated_at": None,
                },
                {
                    "created_at": None,
                    "id": 2,
                    "name": "test2",
                    "updated_at": None,
                },
                {
                    "created_at": None,
                    "id": 3,
                    "name": "test3",
                    "updated_at": None,
                },
                {
                    "created_at": None,
                    "id": 4,
                    "name": "test4",
                    "updated_at": None,
                },
            ]

    def test_add_wrong_column(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                "Invalid columns ['email'] for model 'MockModel'. Valid columns are: ['name', 'created_at', 'updated_at']"
            ),
        ):
            mysql_alchemy.add(MockModel, [{"email": "test3"}, {"name": "test4"}])

    def test_add_session_error(self, mysql_alchemy: MySQLAlchemy):
        with patch.object(mysql_alchemy, "get_session") as mock_get_session:
            mock_session = Mock()
            mock_session.add_all.side_effect = Exception("Add error")
            mock_get_session.return_value.__enter__.return_value = mock_session
            assert mysql_alchemy.add(MockModel, [{"name": "test3"}]) == {
                "success": False,
                "error": "Add error",
            }

    def test_add_model_instances(self, mysql_alchemy: MySQLAlchemy):
        new_instance = MockModel(name="test3")
        result = mysql_alchemy.add(MockModel, [new_instance])
        assert result == {"success": True}
        results = mysql_alchemy.get(MockModel)
        assert results == [
            {"id": 1, "name": "test1", "created_at": None, "updated_at": None},
            {"id": 2, "name": "test2", "created_at": None, "updated_at": None},
            {"id": 3, "name": "test3", "created_at": None, "updated_at": None},
        ]

    def test_add_invalid_model_instance(self, mysql_alchemy: MySQLAlchemy):
        class AnotherModel(StandardModel):
            __tablename__ = "another_table"
            id = Column(Integer, primary_key=True)
            name = Column(String(100))

        new_instance = AnotherModel(name="test3")
        with pytest.raises(
            AssertionError,
            match=re.escape(
                "The data in position 0 is invalid (AnotherModel). It must be a dict or a MockModel."
            ),
        ):
            mysql_alchemy.add(MockModel, [new_instance])

    def test_add_model_instances_invalid_primary_key(self, mysql_alchemy: MySQLAlchemy):
        new_instance = MockModel(id=1, name="test3")
        with pytest.raises(
            AssertionError,
            match="Primary key column 'id' cannot be included when adding new instances.",
        ):
            mysql_alchemy.add(MockModel, [new_instance])


class TestUpdate:
    def test_update_invalid_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                f"The model {InvalidModel} is not mapped. Valid models are: {['MockModel']}.\n\n"
                "Use self.models.all_models() to check valid models."
            ),
        ):
            mysql_alchemy.update(InvalidModel, {"name": "test1_updated"})

    def test_update_with_query(self, mysql_alchemy: MySQLAlchemy):
        updated_count = mysql_alchemy.update(MockModel, {"name": "test1_updated"}, id=1)
        assert updated_count == 1
        columns_to_return = [
            getattr(MockModel, column_name) for column_name in MockModel.columns()
        ]
        with mysql_alchemy.SessionLocal() as session:
            results = session.execute(select(*columns_to_return)).all()
            results = [dict(zip(MockModel.columns(), row)) for row in results]
            assert results == [
                {
                    "created_at": None,
                    "id": 1,
                    "name": "test1_updated",
                    "updated_at": results[0]["updated_at"],
                },
                {
                    "created_at": None,
                    "id": 2,
                    "name": "test2",
                    "updated_at": None,
                },
            ]

    def test_update_all(self, mysql_alchemy: MySQLAlchemy):
        updated_count = mysql_alchemy.update(MockModel, {"name": "test1_updated"})
        assert updated_count == 2
        columns_to_return = [
            getattr(MockModel, column_name) for column_name in MockModel.columns()
        ]
        with mysql_alchemy.SessionLocal() as session:
            results = session.execute(select(*columns_to_return)).all()
            results = [dict(zip(MockModel.columns(), row)) for row in results]
            assert results == [
                {
                    "created_at": None,
                    "id": 1,
                    "name": "test1_updated",
                    "updated_at": results[0]["updated_at"],
                },
                {
                    "created_at": None,
                    "id": 2,
                    "name": "test1_updated",
                    "updated_at": results[1]["updated_at"],
                },
            ]

    def test_update_query_no_values(self, mysql_alchemy: MySQLAlchemy):
        updated_count = mysql_alchemy.update(
            MockModel, {"name": "test1_updated"}, id=100
        )
        assert updated_count == 0
        columns_to_return = [
            getattr(MockModel, column_name) for column_name in MockModel.columns()
        ]
        with mysql_alchemy.SessionLocal() as session:
            results = session.execute(select(*columns_to_return)).all()
            results = [dict(zip(MockModel.columns(), row)) for row in results]
            assert results == [
                {
                    "created_at": None,
                    "id": 1,
                    "name": "test1",
                    "updated_at": None,
                },
                {
                    "created_at": None,
                    "id": 2,
                    "name": "test2",
                    "updated_at": None,
                },
            ]


class TestCount:
    def test_count_invalid_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                f"The model {InvalidModel} is not mapped. Valid models are: {['MockModel']}.\n\n"
                "Use self.models.all_models() to check valid models."
            ),
        ):
            mysql_alchemy.count(InvalidModel)

    def test_count_all(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.count(MockModel) == 2

    def test_count_query(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.count(MockModel, name="test1") == 1

    def test_count_query_no_values(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.count(MockModel, name="test10") == 0


class TestDelete:
    def test_delete_invalid_model(self, mysql_alchemy: MySQLAlchemy):
        with pytest.raises(
            AssertionError,
            match=re.escape(
                f"The model {InvalidModel} is not mapped. Valid models are: {['MockModel']}.\n\n"
                "Use self.models.all_models() to check valid models."
            ),
        ):
            mysql_alchemy.delete(InvalidModel)

    def test_delete_all(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.delete(MockModel) == 2

    def test_delete_with_query(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.delete(MockModel, name="test1") == 1

    def test_delete_query_no_values(self, mysql_alchemy: MySQLAlchemy):
        assert mysql_alchemy.delete(MockModel, name="non_existent") == 0
