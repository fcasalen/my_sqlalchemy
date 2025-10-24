import re
import uuid

import pytest
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from src.my_sqlalchemy.models_handler import ModelsHandler
from src.my_sqlalchemy.standard_model import StandardModel

# Create a separate base for this test module to avoid conflicts
_TestBase = declarative_base()


class ValidModel(StandardModel):
    __tablename__ = f"valid_model_{uuid.uuid4().hex[:8]}"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    metadata = _TestBase.metadata


class InvalidModel(_TestBase):
    __tablename__ = f"invalid_model_{uuid.uuid4().hex[:8]}"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))


class TestModelsHandler:
    def test_valid_models_initialization(self):
        """Test successful initialization with valid models"""
        handler = ModelsHandler(models_list=[ValidModel])
        assert handler.models_list == [ValidModel]
        assert hasattr(handler, "ValidModel")
        assert handler.ValidModel == ValidModel
        assert handler.models_list_names == ["ValidModel"]

    def test_invalid_model_raises_error_in_post_init(self):
        """Test that invalid models raise ValueError in __post_init__"""
        with pytest.raises(
            ValueError, match="Model InvalidModel must inherit from StandardModel"
        ):
            ModelsHandler(models_list=[InvalidModel])

    def test_mixed_models_raises_error(self):
        """Test that mixing valid and invalid models raises error"""
        with pytest.raises(
            ValueError, match="Model InvalidModel must inherit from StandardModel"
        ):
            ModelsHandler(models_list=[ValidModel, InvalidModel])

    def test_empty_models_list(self):
        """Test initialization with empty models list"""
        with pytest.raises(ValueError, match="models_list cannot be empty"):
            ModelsHandler(models_list=[])

    def test_multiple_valid_models(self):
        """Test initialization with multiple valid models"""

        class AnotherValidModel(StandardModel):
            __tablename__ = f"another_valid_model_{uuid.uuid4().hex[:8]}"
            id = Column(Integer, primary_key=True)
            metadata = _TestBase.metadata

        handler = ModelsHandler(models_list=[ValidModel, AnotherValidModel])
        assert handler.models_list == [ValidModel, AnotherValidModel]
        assert hasattr(handler, "ValidModel")
        assert hasattr(handler, "AnotherValidModel")
        assert handler.models_list_names == ["ValidModel", "AnotherValidModel"]

    def test_model_validator_with_invalid_data_structure(self):
        """Test model validator with invalid data structure"""
        # The validator checks for 'oxe' key instead of 'models_list' due to the bug
        invalid_data = {"oxe": [InvalidModel]}
        with pytest.raises(
            ValueError,
            match=re.escape(
                "1 validation error for ModelsHandler\nmodels_list\n  Field required [type=missing"
            ),
        ):
            ModelsHandler.model_validate(invalid_data)
