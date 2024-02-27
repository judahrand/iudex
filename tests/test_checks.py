import pyarrow
import pytest

import iudex.checks


def test_all():
    check = iudex.checks.All({iudex.checks.Greater(0), iudex.checks.Less(4)})

    col = pyarrow.array([1, 2, 3])
    assert check(col).to_pylist() == [True, True, True]

    col = pyarrow.array([-1, 2, 3])
    assert check(col).to_pylist() == [False, True, True]


def test_any():
    check = iudex.checks.Any_({iudex.checks.Less(0), iudex.checks.Greater(0)})

    col = pyarrow.array([-1, 1, 2])
    assert check(col).to_pylist() == [True, True, True]

    col = pyarrow.array([-1, 0, 1])
    assert check(col).to_pylist() == [True, False, True]


def test_greater():
    check = iudex.checks.Greater(0)

    col = pyarrow.array([1, 2, 3])
    assert check(col).to_pylist() == [True, True, True]

    col = pyarrow.array([-1, 0, 1])
    assert check(col).to_pylist() == [False, False, True]


def test_greater_equal():
    check = iudex.checks.GreaterEqual(0)

    col = pyarrow.array([1, 2, 3])
    assert check(col).to_pylist() == [True, True, True]

    col = pyarrow.array([-1, 0, 1])
    assert check(col).to_pylist() == [False, True, True]


def test_less():
    check = iudex.checks.Less(0)

    col = pyarrow.array([-1, -2, -3])
    assert check(col).to_pylist() == [True, True, True]

    col = pyarrow.array([-1, 2, -3])
    assert check(col).to_pylist() == [True, False, True]


def test_less_equal():
    check = iudex.checks.LessEqual(0)

    col = pyarrow.array([-1, 0, -2])
    assert check(col).to_pylist() == [True, True, True]

    col = pyarrow.array([1, 0, -3])
    assert check(col).to_pylist() == [False, True, True]


def test_is_in():
    check = iudex.checks.IsIn({1, 2, 3, None})

    col = pyarrow.array([1, 2, 3, None])
    assert check(col).to_pylist() == [True, True, True, True]

    col = pyarrow.array([1, 2, 4, None])
    assert check(col).to_pylist() == [True, True, False, True]


def test_is_in_empty():
    with pytest.raises(
        ValueError,
        match=r"Value set must contain at least one value.",
    ):
        iudex.checks.IsIn(set())


def test_not_in():
    check = iudex.checks.NotIn({1, 2, 3, None})

    col = pyarrow.array([1, 2, 4, None])
    assert check(col).to_pylist() == [False, False, True, False]

    col = pyarrow.array([5, 6, 7])
    assert check(col).to_pylist() == [True, True, True]


def test_not_in_empty():
    with pytest.raises(
        ValueError,
        match=r"Value set must contain at least one value.",
    ):
        iudex.checks.NotIn(set())
