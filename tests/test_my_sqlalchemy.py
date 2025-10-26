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


class TestInit:
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
    def test_construct_where_clause(self, mysql_alchemy):
        stmt = select(MockModel)
        query_kwargs = {"name": "test", "id": 1}

        result = mysql_alchemy._construct_where_clause(stmt, MockModel, query_kwargs)

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
        assert mysql_alchemy.add(
            MockModel, [{"email": "test3"}, {"name": "test4"}]
        ) == {
            "success": False,
            "error": "'email' is an invalid keyword argument for MockModel",
        }


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
