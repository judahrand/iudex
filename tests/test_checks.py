import pyarrow
import pytest
from typing import Any

import iudex.checks


def make_dataset(data: list[Any]) -> pyarrow.dataset.Dataset:
    return pyarrow.dataset.dataset(
        pyarrow.table({"a": data}),
    )


def test_all():
    check = iudex.checks.All({iudex.checks.Greater(0), iudex.checks.Less(4)})

    ds = make_dataset([1, 2, 3])
    assert check(ds, "a").to_pylist() == [True, True, True]

    ds = make_dataset([-1, 2, 3])
    assert check(ds, "a").to_pylist() == [False, True, True]


def test_any():
    check = iudex.checks.Any_({iudex.checks.Less(0), iudex.checks.Greater(0)})

    ds = make_dataset([-1, 1, 2])
    assert check(ds, "a").to_pylist() == [True, True, True]

    ds = make_dataset([-1, 0, 1])
    assert check(ds, "a").to_pylist() == [True, False, True]


def test_unique():
    check = iudex.checks.Unique()

    ds = make_dataset([1, 2, 3])
    assert check(ds, "a").to_pylist() == [True, True, True]

    ds = make_dataset([1, 2, 1])
    assert check(ds, "a").to_pylist() == [False, True, False]


def test_greater():
    check = iudex.checks.Greater(0)

    ds = make_dataset([1, 2, 3])
    assert check(ds, "a").to_pylist() == [True, True, True]

    ds = make_dataset([-1, 0, 1])
    assert check(ds, "a").to_pylist() == [False, False, True]


def test_greater_equal():
    check = iudex.checks.GreaterEqual(0)

    ds = make_dataset([1, 2, 3])
    assert check(ds, "a").to_pylist() == [True, True, True]

    ds = make_dataset([-1, 0, 1])
    assert check(ds, "a").to_pylist() == [False, True, True]


def test_less():
    check = iudex.checks.Less(0)

    ds = make_dataset([-1, -2, -3])
    assert check(ds, "a").to_pylist() == [True, True, True]

    ds = make_dataset([-1, 2, -3])
    assert check(ds, "a").to_pylist() == [True, False, True]


def test_less_equal():
    check = iudex.checks.LessEqual(0)

    ds = make_dataset([-1, 0, -2])
    assert check(ds, "a").to_pylist() == [True, True, True]

    ds = make_dataset([1, 0, -3])
    assert check(ds, "a").to_pylist() == [False, True, True]


def test_is_in():
    check = iudex.checks.IsIn({1, 2, 3, None})

    ds = make_dataset([1, 2, 3, None])
    assert check(ds, "a").to_pylist() == [True, True, True, True]

    ds = make_dataset([1, 2, 4, None])
    assert check(ds, "a").to_pylist() == [True, True, False, True]


def test_is_in_empty():
    with pytest.raises(
        ValueError,
        match=r"Value set must contain at least one value.",
    ):
        iudex.checks.IsIn(set())


def test_not_in():
    check = iudex.checks.NotIn({1, 2, 3, None})

    ds = make_dataset([1, 2, 4, None])
    assert check(ds, "a").to_pylist() == [False, False, True, False]

    ds = make_dataset([5, 6, 7])
    assert check(ds, "a").to_pylist() == [True, True, True]


def test_not_in_empty():
    with pytest.raises(
        ValueError,
        match=r"Value set must contain at least one value.",
    ):
        iudex.checks.NotIn(set())
