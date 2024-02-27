from __future__ import annotations

import dataclasses
import functools
from abc import ABC, abstractmethod
from collections.abc import Set
from typing import Any

import ibis
import ibis.expr.types
import pyarrow.compute


class Check(ABC):
    @abstractmethod
    def __call__(self, column: ibis.expr.types.Column) -> pyarrow.BooleanArray:
        ...

    def __and__(self, other: Check) -> Check:
        return All(frozenset({self, other}))

    def __or__(self, other: Check) -> Check:
        return Any_(frozenset({self, other}))


@dataclasses.dataclass(frozen=True)
class All(Check):
    checks: Set[Check]

    def __call__(self, column: ibis.expr.types.Column) -> pyarrow.BooleanArray:
        return functools.reduce(
            pyarrow.compute.and_,
            (check(column) for check in self.checks),
        )


@dataclasses.dataclass(frozen=True)
class Any_(Check):
    checks: Set[Check]

    def __call__(self, column: ibis.expr.types.Column) -> pyarrow.BooleanArray:
        return functools.reduce(
            pyarrow.compute.or_,
            (check(column) for check in self.checks),
        )


@dataclasses.dataclass(frozen=True)
class Unique(Check):
    def __call__(self, column: ibis.expr.types.Column) -> pyarrow.BooleanArray:
        column = column.name("col")
        unique_map = column.value_counts().select(
            "col",
            unique=ibis._["col_count"] == 1,
        )
        return column.as_table().join(unique_map, "col")["unique"].to_pyarrow()


@dataclasses.dataclass(frozen=True)
class Greater(Check):
    value: Any

    def __call__(self, column: ibis.expr.types.Column) -> pyarrow.BooleanArray:
        return (column > ibis.literal(self.value)).to_pyarrow()


@dataclasses.dataclass(frozen=True)
class GreaterEqual(Check):
    value: Any

    def __call__(self, column: ibis.expr.types.Column) -> pyarrow.BooleanArray:
        return (column >= ibis.literal(self.value)).to_pyarrow()


@dataclasses.dataclass(frozen=True)
class Less(Check):
    value: Any

    def __call__(self, column: ibis.expr.types.Column) -> pyarrow.BooleanArray:
        return (column < ibis.literal(self.value)).to_pyarrow()


@dataclasses.dataclass(frozen=True)
class LessEqual(Check):
    value: Any

    def __call__(self, column: ibis.expr.types.Column) -> pyarrow.BooleanArray:
        return (column <= ibis.literal(self.value)).to_pyarrow()


@dataclasses.dataclass
class IsIn(Check):
    values: Set[Any]
    skip_nulls: bool = False

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("Value set must contain at least one value.")

    def __call__(self, column: ibis.expr.types.Column) -> pyarrow.BooleanArray:
        res = column.isin(ibis.literal(self.values))

        if None in self.values or self.skip_nulls:
            res = res.fillna(True)
        else:
            res = res.fillna(False)

        return res.to_pyarrow()


@dataclasses.dataclass(frozen=True)
class NotIn(Check):
    values: Set[Any]
    skip_nulls: bool = False

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("Value set must contain at least one value.")

    def __call__(self, column: ibis.expr.types.Column) -> pyarrow.BooleanArray:
        res = column.notin(ibis.literal(self.values))

        if None not in self.values or self.skip_nulls:
            res = res.fillna(True)
        else:
            res = res.fillna(False)

        return res.to_pyarrow()
