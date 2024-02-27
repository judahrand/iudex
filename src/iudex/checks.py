from __future__ import annotations

import dataclasses
import functools
from abc import ABC, abstractmethod
from collections.abc import Set
from typing import Any

import pyarrow
import pyarrow.compute


class Check(ABC):
    @abstractmethod
    def __call__(
        self, column: pyarrow.Array | pyarrow.ChunkedArray
    ) -> pyarrow.BooleanArray:
        ...

    def __and__(self, other: Check) -> Check:
        return All(frozenset({self, other}))

    def __or__(self, other: Check) -> Check:
        return Any_(frozenset({self, other}))


@dataclasses.dataclass(frozen=True)
class All(Check):
    checks: Set[Check]

    def __call__(
        self, column: pyarrow.Array | pyarrow.ChunkedArray
    ) -> pyarrow.BooleanArray:
        return functools.reduce(
            pyarrow.compute.and_,
            (check(column) for check in self.checks),
        )


@dataclasses.dataclass(frozen=True)
class Any_(Check):
    checks: Set[Check]

    def __call__(
        self, column: pyarrow.Array | pyarrow.ChunkedArray
    ) -> pyarrow.BooleanArray:
        return functools.reduce(
            pyarrow.compute.or_,
            (check(column) for check in self.checks),
        )


@dataclasses.dataclass(frozen=True)
class Greater(Check):
    value: Any

    def __call__(
        self, column: pyarrow.Array | pyarrow.ChunkedArray
    ) -> pyarrow.BooleanArray:
        return pyarrow.compute.greater(column, self.value)


@dataclasses.dataclass(frozen=True)
class GreaterEqual(Check):
    value: Any

    def __call__(
        self, column: pyarrow.Array | pyarrow.ChunkedArray
    ) -> pyarrow.BooleanArray:
        return pyarrow.compute.greater_equal(column, self.value)


@dataclasses.dataclass(frozen=True)
class Less(Check):
    value: Any

    def __call__(
        self, column: pyarrow.Array | pyarrow.ChunkedArray
    ) -> pyarrow.BooleanArray:
        return pyarrow.compute.less(column, self.value)


@dataclasses.dataclass(frozen=True)
class LessEqual(Check):
    value: Any

    def __call__(
        self, column: pyarrow.Array | pyarrow.ChunkedArray
    ) -> pyarrow.BooleanArray:
        return pyarrow.compute.less_equal(column, self.value)


@dataclasses.dataclass(frozen=True)
class IsIn(Check):
    values: Set[Any]

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("Value set must contain at least one value.")

    def __call__(
        self, column: pyarrow.Array | pyarrow.ChunkedArray
    ) -> pyarrow.BooleanArray:
        return pyarrow.compute.is_in(column, pyarrow.array(self.values))


@dataclasses.dataclass(frozen=True)
class NotIn(Check):
    values: Set[Any]

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("Value set must contain at least one value.")

    def __call__(
        self, column: pyarrow.Array | pyarrow.ChunkedArray
    ) -> pyarrow.BooleanArray:
        return pyarrow.compute.invert(IsIn(self.values)(column))
