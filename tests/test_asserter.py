import pytest
from sqlalchemy.orm import DeclarativeMeta

from src.my_sqlalchemy import asserter


def test_list_of_models_no_metadata():
    with pytest.raises(
        ValueError,
        match="base_metadata must be provided when type_ is DeclarativeMeta.",
    ):
        asserter.list_of([], DeclarativeMeta)
