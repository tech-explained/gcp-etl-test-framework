"""Validator registry: metadata `type:` -> validator class."""
from __future__ import annotations

from .base import BaseValidator
from .data_quality import NotNullValidator, ReferentialIntegrityValidator, UniqueValidator
from .file_checks import GCSFileExistsValidator, RowCountValidator
from .scd2 import SCD2Validator
from .scd4 import SCD4Validator
from .schema import SchemaValidator

REGISTRY = {
    cls.type_name: cls
    for cls in (
        GCSFileExistsValidator,
        RowCountValidator,
        SchemaValidator,
        NotNullValidator,
        UniqueValidator,
        ReferentialIntegrityValidator,
        SCD2Validator,
        SCD4Validator,
    )
}


def get_validator(type_name: str) -> type[BaseValidator]:
    try:
        return REGISTRY[type_name]
    except KeyError:
        raise ValueError(
            f"unknown validator type: {type_name!r}. Known types: {sorted(REGISTRY)}"
        )
