from unittest.mock import Mock, patch

import pytest
from sqlalchemy import Column, DateTime, Integer, String, select
from sqlalchemy.orm import Session, declarative_base

from src.my_sqlalchemy.models_handler import ModelsHandler
from src.my_sqlalchemy.my_sqlalchemy import MySQLAlchemy
from src.my_sqlalchemy.standard_model import StandardModel

# Test model
Base = declarative_base()


class MockModel(StandardModel):
    __tablename__ = "test_table"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


@pytest.fixture
def test_models():
    return [MockModel]


@pytest.fixture
def mysql_alchemy(test_models):
    return MySQLAlchemy("sqlite:///:memory:", test_models)


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

    def test_assert_model_valid(self, mysql_alchemy):
        # Should not raise
        mysql_alchemy._assert_model(MockModel)

    def test_assert_model_invalid(self, mysql_alchemy):
        class InvalidModel:
            pass

        with pytest.raises(AssertionError):
            mysql_alchemy._assert_model(InvalidModel)


class TestGet:
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

    @patch("src.my_sqlalchemy.my_sqlalchemy.MySQLAlchemy.get_session")
    def test_get(
        self, mock_get_session, mysql_alchemy: MySQLAlchemy, mock_session: Session
    ):
        mock_result = Mock()
        mock_result.scalars.return_value.all.return_value = [Mock(), Mock()]
        mock_session.execute.return_value = mock_result

        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_get_session.return_value.__exit__.return_value = None

        with patch.object(
            mysql_alchemy, "_expunge_objects", return_value=[]
        ) as mock_expunge:
            mysql_alchemy.get(MockModel, name="test")

            mock_session.execute.assert_called_once()
            mock_expunge.assert_called_once()

    @patch("src.my_sqlalchemy.my_sqlalchemy.MySQLAlchemy.get_session")
    def test_get_with_limit(
        self, mock_get_session, mysql_alchemy: MySQLAlchemy, mock_session: Session
    ):
        mock_result = Mock()
        mock_result.scalars.return_value.all.return_value = [Mock()]
        mock_session.execute.return_value = mock_result

        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_get_session.return_value.__exit__.return_value = None

        with patch.object(mysql_alchemy, "_expunge_objects", return_value=[]):
            mysql_alchemy.get(MockModel, limit=5, name="test")

            mock_session.execute.assert_called_once()


class TestExpunge:
    def test_expunge_object(self, mysql_alchemy: MySQLAlchemy, mock_session: Session):
        obj = Mock()
        obj.id = 1
        obj.created_at = None
        obj.updated_at = None

        result = mysql_alchemy._expunge_object(mock_session, obj)

        assert result == obj
        mock_session.expunge.assert_called_once_with(obj)

    def test_expunge_object_none(
        self, mysql_alchemy: MySQLAlchemy, mock_session: Session
    ):
        result = mysql_alchemy._expunge_object(mock_session, None)
        assert result is None
        mock_session.expunge.assert_not_called()

    def test_expunge_objects(self, mysql_alchemy: MySQLAlchemy, mock_session: Session):
        obj1 = Mock()
        obj1.id = 1
        obj2 = Mock()
        obj2.id = 2
        objects = [obj1, obj2]

        result = mysql_alchemy._expunge_objects(mock_session, objects)

        assert result == objects
        assert mock_session.expunge.call_count == 2


class TestForceLoadAttributes:
    def test_force_load_attributes(self, mysql_alchemy):
        obj = Mock()
        obj.id = 1
        obj.created_at = None
        obj.updated_at = None

        # Should not raise exception
        mysql_alchemy._force_load_attributes(obj)

    def test_force_load_attributes_exception(self, mysql_alchemy):
        obj = Mock()
        obj.id = Mock(side_effect=Exception("Test error"))

        # Should not raise exception even if attributes fail
        mysql_alchemy._force_load_attributes(obj)


class TestConstructWhereClause:
    def test_construct_where_clause(self, mysql_alchemy):
        stmt = select(MockModel)
        query_kwargs = {"name": "test", "id": 1}

        result = mysql_alchemy._construct_where_clause(stmt, MockModel, query_kwargs)

        assert result is not None


class TestAdd:
    @patch("src.my_sqlalchemy.my_sqlalchemy.MySQLAlchemy.get_session")
    def test_add(
        self, mock_get_session, mysql_alchemy: MySQLAlchemy, mock_session: Session
    ):
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_get_session.return_value.__exit__.return_value = None

        data = [{"name": "test1"}, {"name": "test2"}]

        mysql_alchemy.add(MockModel, data)

        mock_session.add_all.assert_called_once()
        mock_session.flush.assert_called_once()


class TestUpdate:
    @patch("src.my_sqlalchemy.my_sqlalchemy.utils.utc_now")
    @patch("src.my_sqlalchemy.my_sqlalchemy.MySQLAlchemy.get_session")
    def test_update(
        self,
        mock_get_session,
        mock_utc_now,
        mysql_alchemy: MySQLAlchemy,
        mock_session: Session,
    ):
        mock_utc_now.return_value = "2023-01-01"
        mock_result = Mock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result

        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_get_session.return_value.__exit__.return_value = None

        result = mysql_alchemy.update(MockModel, {"name": "updated"}, id=1)

        assert result == 1
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()


class TestCount:
    @patch("src.my_sqlalchemy.my_sqlalchemy.MySQLAlchemy.get_session")
    def test_count(
        self, mock_get_session, mysql_alchemy: MySQLAlchemy, mock_session: Session
    ):
        mock_session.execute.return_value.scalar.return_value = 5

        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_get_session.return_value.__exit__.return_value = None

        result = mysql_alchemy.count(MockModel, name="test")

        assert result == 5
        mock_session.execute.assert_called_once()


class TestDelete:
    @patch("src.my_sqlalchemy.my_sqlalchemy.MySQLAlchemy.get_session")
    def test_delete_success(
        self, mock_get_session, mysql_alchemy: MySQLAlchemy, mock_session: Session
    ):
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_get_session.return_value.__exit__.return_value = None

        mock_result = Mock()
        mock_result.rowcount = 2
        mock_session.execute.return_value = mock_result

        result = mysql_alchemy.delete(MockModel, id=1)

        assert result == 2
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("src.my_sqlalchemy.my_sqlalchemy.MySQLAlchemy.get_session")
    def test_delete_no_rows(
        self, mock_get_session, mysql_alchemy: MySQLAlchemy, mock_session: Session
    ):
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_get_session.return_value.__exit__.return_value = None

        mock_result = Mock()
        mock_result.rowcount = 0
        mock_session.execute.return_value = mock_result

        result = mysql_alchemy.delete(MockModel, id=999)

        assert result == 0
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()

    def test_delete_invalid_model(self, mysql_alchemy):
        class InvalidModel:
            pass

        with pytest.raises(AssertionError):
            mysql_alchemy.delete(InvalidModel, id=1)
