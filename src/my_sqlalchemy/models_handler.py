from dataclasses import dataclass, field
from typing import List

from sqlalchemy.orm.decl_api import DeclarativeMeta

from .standard_model import StandardModel


@dataclass
class ModelsHandler:
    """A custom models handler using dataclasses"""

    models_list: List[DeclarativeMeta]
    models_list_names: List[str] = field(init=False)

    def __post_init__(self):
        """Validate models and set up dynamic attributes after initialization"""
        if not isinstance(self.models_list, list):
            raise TypeError("models_list must be a list")

        if not self.models_list:
            raise ValueError("models_list cannot be empty")

        for model in self.models_list:
            if not isinstance(model, type) or not issubclass(model, StandardModel):
                model_name = getattr(model, "__name__", str(model))
                raise ValueError(f"Model {model_name} must inherit from StandardModel")

        models_list_names = []
        for model in self.models_list:
            setattr(self, model.__name__, model)
            models_list_names.append(model.__name__)

        self.models_list_names = models_list_names

    @classmethod
    def model_validate(cls, data: dict) -> "ModelsHandler":
        """
        Class method to validate data and create ModelsHandler instance.
        This provides compatibility with the previous Pydantic-style interface.

        Args:
            data (dict): Dictionary containing 'models_list' key.

        Returns:
            ModelsHandler: An instance of ModelsHandler.
        """
        if not isinstance(data, dict):
            raise TypeError("Data must be a dictionary")

        if "models_list" not in data:
            raise ValueError("models_list key is required in data")

        return cls(models_list=data["models_list"])
