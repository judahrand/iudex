from __future__ import annotations

import dataclasses
import functools
from abc import ABC, abstractmethod
from collections.abc import Set
from typing import Any

import pyarrow
import pyarrow.compute
import pyarrow.dataset


class Check(ABC):
    @abstractmethod
    def __call__(
        self,
        data: pyarrow.dataset.Dataset,
        column: str,
    ) -> pyarrow.ChunkedArray:
        ...

    def __and__(self, other: Check) -> Check:
        return All(frozenset({self, other}))

    def __or__(self, other: Check) -> Check:
        return Any_(frozenset({self, other}))


@dataclasses.dataclass(frozen=True)
class All(Check):
    checks: Set[Check]

    def __call__(
        self,
        data: pyarrow.dataset.Dataset,
        column: str,
    ) -> pyarrow.ChunkedArray:
        return functools.reduce(
            pyarrow.compute.and_,
            (check(data, column) for check in self.checks),
        )


@dataclasses.dataclass(frozen=True)
class Any_(Check):
    checks: Set[Check]

    def __call__(
        self,
        data: pyarrow.dataset.Dataset,
        column: str,
    ) -> pyarrow.ChunkedArray:
        return functools.reduce(
            pyarrow.compute.or_,
            (check(data, column) for check in self.checks),
        )


@dataclasses.dataclass(frozen=True)
class Greater(Check):
    value: Any

    def __call__(
        self,
        data: pyarrow.dataset.Dataset,
        column: str,
    ) -> pyarrow.ChunkedArray:
        arrays = []
        for batch in data.to_batches(columns=[column]):
            arrays.append(pyarrow.compute.greater(batch.column(0), self.value))
        return pyarrow.chunked_array(arrays)


@dataclasses.dataclass(frozen=True)
class GreaterEqual(Check):
    value: Any

    def __call__(
        self,
        data: pyarrow.dataset.Dataset,
        column: str,
    ) -> pyarrow.ChunkedArray:
        arrays = []
        for batch in data.to_batches(columns=[column]):
            arrays.append(pyarrow.compute.greater_equal(batch.column(0), self.value))
        return pyarrow.chunked_array(arrays)


@dataclasses.dataclass(frozen=True)
class Less(Check):
    value: Any

    def __call__(
        self,
        data: pyarrow.dataset.Dataset,
        column: str,
    ) -> pyarrow.ChunkedArray:
        arrays = []
        for batch in data.to_batches(columns=[column]):
            arrays.append(pyarrow.compute.less(batch.column(0), self.value))
        return pyarrow.chunked_array(arrays)


@dataclasses.dataclass(frozen=True)
class LessEqual(Check):
    value: Any

    def __call__(
        self,
        data: pyarrow.dataset.Dataset,
        column: str,
    ) -> pyarrow.ChunkedArray:
        arrays = []
        for batch in data.to_batches(columns=[column]):
            arrays.append(pyarrow.compute.less_equal(batch.column(0), self.value))
        return pyarrow.chunked_array(arrays)


@dataclasses.dataclass(frozen=True)
class IsIn(Check):
    values: Set[Any]

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("Value set must contain at least one value.")

    def __call__(
        self,
        data: pyarrow.dataset.Dataset,
        column: str,
    ) -> pyarrow.ChunkedArray:
        arrays = []
        for batch in data.to_batches(columns=[column]):
            arrays.append(
                pyarrow.compute.is_in(batch.column(0), pyarrow.array(self.values)),
            )
        return pyarrow.chunked_array(arrays)


@dataclasses.dataclass(frozen=True)
class NotIn(Check):
    values: Set[Any]

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("Value set must contain at least one value.")

    def __call__(
        self,
        data: pyarrow.dataset.Dataset,
        column: str,
    ) -> pyarrow.ChunkedArray:
        return pyarrow.compute.invert(IsIn(self.values)(data, column))
