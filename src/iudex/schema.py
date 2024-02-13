from __future__ import annotations

import dataclasses
from collections.abc import Set

import pyarrow
import ibis.expr.datatypes

from .checks import Check


@dataclasses.dataclass(frozen=True)
class Schema:
    """A schema for a DataFrame."""

    fields: Set[Field]

    def to_ibis(self) -> ibis.expr.schema.Schema:
        """Convert to an Ibis schema."""
        return ibis.Schema(
            {field.name: field.data_type for field in self.fields},
        )

    def to_pyarrow(self) -> pyarrow.Schema:
        """Convert to a PyArrow schema."""
        return pyarrow.schema([field.to_pyarrow() for field in self.fields])


@dataclasses.dataclass(frozen=True)
class Field:
    """A field in a schema."""

    name: str
    data_type: ibis.expr.datatypes.DataType
    checks: Set[Check] = dataclasses.field(default_factory=set)

    def to_pyarrow(self) -> pyarrow.Field:
        """Convert to an PyArrow field."""
        return pyarrow.field(
            self.name,
            self.data_type.to_pyarrow(),
            nullable=self.data_type.nullable,
        )
