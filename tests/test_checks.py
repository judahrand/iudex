import pyarrow
import pytest
import ibis

import iudex.checks


def make_column(array):
    return ibis.memtable(pyarrow.Table.from_arrays([array], names=["a"]))["a"]


def test_unique():
    check = iudex.checks.Unique()

    col = make_column(pyarrow.array([1, 2, 3]))
    assert check(col) is True

    col = make_column(pyarrow.array([1, 2, 3, 3]))
    assert check(col) is False


def test_greater():
    check = iudex.checks.Greater(0)

    col = make_column(pyarrow.array([1, 2, 3]))
    assert check(col) is True

    col = make_column(pyarrow.array([-1, 0, 1]))
    assert check(col) is False


def test_greater_equal():
    check = iudex.checks.GreaterEqual(0)

    col = make_column(pyarrow.array([1, 2, 3]))
    assert check(col) is True

    col = make_column(pyarrow.array([-1, 0, 1]))
    assert check(col) is False


def test_less():
    check = iudex.checks.Less(0)

    col = make_column(pyarrow.array([-1, -2, -3]))
    assert check(col) is True

    col = make_column(pyarrow.array([-1, 2, -3]))
    assert check(col) is False


def test_less_equal():
    check = iudex.checks.LessEqual(0)

    col = make_column(pyarrow.array([-1, 0, -2]))
    assert check(col) is True

    col = make_column(pyarrow.array([1, 0, -3]))
    assert check(col) is False


def test_is_in():
    check = iudex.checks.IsIn({1, 2, 3, None})

    col = make_column(pyarrow.array([1, 2, 3, None]))
    assert check(col) is True

    col = make_column(pyarrow.array([1, 2, 4, None]))
    assert check(col) is False


def test_is_in_empty():
    with pytest.raises(
        ValueError,
        match=r"Value set must contain at least one value.",
    ):
        iudex.checks.IsIn(set())


def test_not_in():
    check = iudex.checks.NotIn({1, 2, 3, None})

    col = make_column(pyarrow.array([1, 2, 4, None]))
    assert check(col) is False

    col = make_column(pyarrow.array([5, 6, 7]))
    assert check(col) is True


def test_not_in_skip_nulls():
    check = iudex.checks.NotIn({1, 2, 3, None}, skip_nulls=False)

    col = make_column(pyarrow.array([1, 2, 3, None]))
    assert check(col) is False

    check = iudex.checks.NotIn({10}, skip_nulls=True)
    assert check(col) is True


def test_not_in_empty():
    with pytest.raises(
        ValueError,
        match=r"Value set must contain at least one value.",
    ):
        iudex.checks.NotIn(set())
