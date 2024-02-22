import dataclasses
from collections.abc import Set
from typing import Any, Protocol

import ibis.expr.types
import pyarrow
import pyarrow.compute


class Check(Protocol):
    def __call__(self, column: ibis.expr.types.Column) -> bool:
        ...


@dataclasses.dataclass(frozen=True)
class Unique(Check):
    def __call__(self, column: ibis.expr.types.Column) -> bool:
        return pyarrow.compute.all(
            (column.nunique() == column.count()).to_pyarrow(),
        ).as_py()


@dataclasses.dataclass(frozen=True)
class Greater(Check):
    value: Any

    def __call__(self, column: ibis.expr.types.Column) -> bool:
        return pyarrow.compute.all(
            (column > ibis.literal(self.value)).min().to_pyarrow(),
        ).as_py()


@dataclasses.dataclass(frozen=True)
class GreaterEqual(Check):
    value: Any

    def __call__(self, column: ibis.expr.types.Column) -> bool:
        return pyarrow.compute.all(
            (column >= ibis.literal(self.value)).min().to_pyarrow(),
        ).as_py()


@dataclasses.dataclass(frozen=True)
class Less(Check):
    value: Any

    def __call__(self, column: ibis.expr.types.Column) -> bool:
        return pyarrow.compute.all(
            (column < ibis.literal(self.value)).min().to_pyarrow(),
        ).as_py()


@dataclasses.dataclass(frozen=True)
class LessEqual(Check):
    value: Any

    def __call__(self, column: ibis.expr.types.Column) -> bool:
        return pyarrow.compute.all(
            (column <= ibis.literal(self.value)).min().to_pyarrow(),
        ).as_py()


@dataclasses.dataclass
class IsIn(Check):
    values: Set[Any]
    skip_nulls: bool = False

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("Value set must contain at least one value.")

    def __call__(self, column: ibis.expr.types.Column) -> bool:
        return pyarrow.compute.all(
            column.isin(ibis.literal(self.values)).min().to_pyarrow(),
        ).as_py()


@dataclasses.dataclass(frozen=True)
class NotIn(Check):
    values: Set[Any]
    skip_nulls: bool = False

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("Value set must contain at least one value.")

    def __call__(self, column: ibis.expr.types.Column) -> bool:
        return pyarrow.compute.all(
            column.notin(ibis.literal(self.values)).min().to_pyarrow(),
        ).as_py()


@dataclasses.dataclass(frozen=True)
class Any_(Check):
    checks: Set[Check]

    def __call__(self, column: ibis.expr.types.Column) -> bool:
        return any(check(column) for check in self.checks)
