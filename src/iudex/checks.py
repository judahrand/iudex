import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Set
from typing import Any

import pyarrow
import pyarrow.compute


class Check(ABC):
    @abstractmethod
    def __call__(self, array: pyarrow.Array) -> bool:
        ...


@dataclasses.dataclass(frozen=True)
class Unique(Check):
    def __call__(self, array: pyarrow.Array) -> bool:
        return len(pyarrow.compute.unique(array)) == len(array)


@dataclasses.dataclass(frozen=True)
class Greater(Check):
    value: Any

    def __call__(self, array: pyarrow.Array) -> bool:
        return pyarrow.compute.all(
            pyarrow.compute.greater(array, self.value),
        ).as_py()


@dataclasses.dataclass(frozen=True)
class GreaterEqual(Check):
    value: Any

    def __call__(self, array: pyarrow.Array) -> bool:
        return pyarrow.compute.all(
            pyarrow.compute.greater_equal(array, self.value),
        ).as_py()


@dataclasses.dataclass(frozen=True)
class Less(Check):
    value: Any

    def __call__(self, array: pyarrow.Array) -> bool:
        return pyarrow.compute.all(
            pyarrow.compute.less(array, self.value),
        ).as_py()


@dataclasses.dataclass(frozen=True)
class LessEqual(Check):
    value: Any

    def __call__(self, array: pyarrow.Array) -> bool:
        return pyarrow.compute.all(
            pyarrow.compute.less_equal(array, self.value),
        ).as_py()


@dataclasses.dataclass
class IsIn(Check):
    values: Set[Any]
    skip_nulls: bool = False

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("Value set must contain at least one value.")

    def __call__(self, array: pyarrow.Array) -> bool:
        return pyarrow.compute.all(
            pyarrow.compute.is_in(
                array,
                pyarrow.array(self.values),
                skip_nulls=self.skip_nulls,
            ),
        ).as_py()


@dataclasses.dataclass(frozen=True)
class NotIn(Check):
    values: Set[Any]
    skip_nulls: bool = False

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("Value set must contain at least one value.")

    def __call__(self, array: pyarrow.Array) -> bool:
        return not pyarrow.compute.any(
            pyarrow.compute.is_in(
                array,
                pyarrow.array(self.values),
                skip_nulls=self.skip_nulls,
            ),
        ).as_py()
