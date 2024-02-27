from __future__ import annotations

import dataclasses
from collections.abc import Sequence

import pyarrow

from .checks import Check


@dataclasses.dataclass(frozen=True)
class Schema:
    """A schema for a DataFrame."""

    fields: Sequence[Field]

    def to_pyarrow(self) -> pyarrow.Schema:
        """Convert to a PyArrow schema."""
        return pyarrow.schema([field.to_pyarrow() for field in self.fields])


@dataclasses.dataclass(frozen=True)
class Field:
    """A field in a schema."""

    name: str
    data_type: pyarrow.DataType
    nullable: bool = True
    check: Check | None = None

    def to_pyarrow(self) -> pyarrow.Field:
        """Convert to an PyArrow field."""
        return pyarrow.field(
            self.name,
            self.data_type,
            nullable=self.nullable,
        )
