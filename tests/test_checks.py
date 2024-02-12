import pyarrow
import pytest

import iudex.checks


def test_unique():
    check = iudex.checks.Unique()
    array = pyarrow.array([1, 2, 3])
    assert check(array) is True

    array = pyarrow.array([1, 2, 3, 3])
    assert check(array) is False


def test_greater():
    check = iudex.checks.Greater(0)
    array = pyarrow.array([1, 2, 3])
    assert check(array) is True

    array = pyarrow.array([-1, 0, 1])
    assert check(array) is False


def test_greater_equal():
    check = iudex.checks.GreaterEqual(0)
    array = pyarrow.array([1, 2, 3])
    assert check(array) is True

    array = pyarrow.array([-1, 0, 1])
    assert check(array) is False


def test_less():
    check = iudex.checks.Less(0)
    array = pyarrow.array([-1, -2, -3])
    assert check(array) is True

    array = pyarrow.array([-1, 2, -3])
    assert check(array) is False


def test_less_equal():
    check = iudex.checks.LessEqual(0)
    array = pyarrow.array([-1, 0, -2])
    assert check(array) is True

    array = pyarrow.array([1, 0, -3])
    assert check(array) is False


def test_is_in():
    check = iudex.checks.IsIn({1, 2, 3})
    array = pyarrow.array([1, 2, 3])
    assert check(array) is True

    array = pyarrow.array([1, 2, 4])
    assert check(array) is False


def test_is_in_skip_nulls():
    check = iudex.checks.IsIn({1, 2, 3, None}, skip_nulls=False)
    array = pyarrow.array([1, 2, 3, None])
    assert check(array) is True

    check = iudex.checks.IsIn({1, 2, 3}, skip_nulls=True)
    assert check(array) is False


def test_is_in_empty():
    with pytest.raises(
        ValueError,
        match=r"Value set must contain at least one value.",
    ):
        iudex.checks.IsIn(set())


def test_not_in():
    check = iudex.checks.NotIn({1, 2, 3})
    array = pyarrow.array([1, 2, 3])
    assert check(array) is False

    array = pyarrow.array([5, 6, 7])
    assert check(array) is True


def test_not_in_skip_nulls():
    check = iudex.checks.NotIn({1, 2, 3, None}, skip_nulls=False)
    array = pyarrow.array([1, 2, 3, None])
    assert check(array) is False

    check = iudex.checks.NotIn({10}, skip_nulls=True)
    assert check(array) is True


def test_not_in_empty():
    with pytest.raises(
        ValueError,
        match=r"Value set must contain at least one value.",
    ):
        iudex.checks.NotIn(set())
