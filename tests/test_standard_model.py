import uuid
from datetime import datetime

import pytest
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from src.my_sqlalchemy.standard_model import StandardModel


class TestStandardModel:
    @pytest.fixture
    def engine(self):
        engine = create_engine("sqlite:///:memory:")
        yield engine
        engine.dispose()

    @pytest.fixture
    def test_base(self):
        """Create a fresh declarative base for each test to avoid table conflicts"""
        yield declarative_base()

    @pytest.fixture
    def session(self, engine, test_base):
        test_base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session
        session.close()

    @pytest.fixture
    def concrete_model(self, test_base):
        unique_id = uuid.uuid4().hex[:8]
        table_name = f"test_model_{unique_id}"
        class_name = f"TestModel_{unique_id}"

        TestModel = type(
            class_name,
            (StandardModel,),
            {
                "__tablename__": table_name,
                "name": Column(String(50)),
                "metadata": test_base.metadata,
            },
        )

        return TestModel

    def test_standard_model_is_abstract(self):
        assert StandardModel.__abstract__ is True

    def test_standard_model_has_required_columns(self, concrete_model):
        assert hasattr(concrete_model, "id")
        assert hasattr(concrete_model, "created_at")
        assert hasattr(concrete_model, "updated_at")

    def test_id_column_properties(self, concrete_model):
        id_column = concrete_model.__table__.columns["id"]
        assert id_column.primary_key is True
        assert id_column.autoincrement is True

    def test_created_at_column_properties(self, concrete_model):
        created_at_column = concrete_model.__table__.columns["created_at"]
        assert created_at_column.nullable is False
        assert created_at_column.default is not None

    def test_updated_at_column_properties(self, concrete_model):
        updated_at_column = concrete_model.__table__.columns["updated_at"]
        assert updated_at_column.nullable is False
        assert updated_at_column.default is not None
        assert updated_at_column.onupdate is not None

    def test_instance_creation(self, concrete_model, session, test_base):
        test_base.metadata.create_all(session.bind)

        instance = concrete_model(name="test")
        session.add(instance)
        session.commit()

        assert instance.id is not None
        assert instance.created_at is not None
        assert instance.updated_at is not None
        assert isinstance(instance.created_at, datetime)
        assert isinstance(instance.updated_at, datetime)

    def test_repr_method(self, concrete_model):
        now = datetime.now()
        instance = concrete_model(name="test", created_at=now)
        assert (
            repr(instance)
            == f"<name=test, id=None, created_at={now.isoformat()}, updated_at=None>"
        )

    def test_get_columns_method(self, concrete_model):
        assert concrete_model.get_columns() == {
            "id": {
                "default": None,
                "length": None,
                "nullable": False,
                "type": "INTEGER",
            },
            "created_at": {
                "default": "function utc_now",
                "length": None,
                "nullable": False,
                "type": "DATETIME",
            },
            "name": {
                "default": None,
                "length": 50,
                "nullable": True,
                "type": "VARCHAR(50)",
            },
            "updated_at": {
                "default": "function utc_now",
                "length": None,
                "nullable": False,
                "type": "DATETIME",
            },
        }

    def test_updated_at_changes_on_update(self, concrete_model, session, test_base):
        test_base.metadata.create_all(session.bind)

        instance = concrete_model(name="test")
        session.add(instance)
        session.commit()

        original_updated_at = instance.updated_at
        instance.name = "updated"
        session.commit()

        assert instance.updated_at != original_updated_at
