from __future__ import annotations

import dataclasses
from collections.abc import Set

import pyarrow

from .checks import Check


class Schema:
    """A schema for a DataFrame."""

    def __init__(self, *fields: Field) -> None:
        self.fields = fields

    def to_arrow(self) -> pyarrow.Schema:
        """Convert to an Arrow schema."""
        return pyarrow.schema(
            [field.to_arrow() for field in self.fields],
        )


@dataclasses.dataclass
class Field:
    """A field in a schema."""

    name: str
    data_type: pyarrow.DataType
    nullable: bool = True
    checks: Set[Check] = dataclasses.field(default_factory=set)

    def to_arrow(self) -> pyarrow.Field:
        """Convert to an Arrow field."""
        return pyarrow.field(
            self.name,
            self.data_type,
            nullable=self.nullable,
        )
