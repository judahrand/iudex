import pandas as pd
import pyarrow
import pytest
import ibis.expr.datatypes

import iudex.checks
import iudex.schema
import iudex.validate


def test_validate_pyarrow_pass():
    schema = iudex.schema.Schema(
        {
            iudex.schema.Field(
                "a",
                pyarrow.int64(),
                nullable=False,
                checks=frozenset(
                    {
                        iudex.checks.Unique(),
                        iudex.checks.Greater(0),
                    }
                ),
            ),
            iudex.schema.Field(
                "b",
                pyarrow.float32(),
                checks=frozenset(
                    {
                        iudex.checks.Greater(0),
                        iudex.checks.LessEqual(10),
                    }
                ),
            ),
        }
    )
    table = pyarrow.Table.from_pydict(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 5.0],
        },
        schema=schema.to_pyarrow(),
    )
    res = iudex.validate.validate_pyarrow(table, schema)
    assert table == res


def test_validate_pyarrow_fail():
    schema = iudex.schema.Schema(
        {
            iudex.schema.Field(
                "a",
                pyarrow.int64(),
                checks=frozenset(
                    {
                        iudex.checks.Unique(),
                        iudex.checks.Greater(0),
                    }
                ),
            ),
            iudex.schema.Field(
                "b",
                pyarrow.float64(),
                checks=frozenset(
                    {
                        iudex.checks.Greater(0),
                        iudex.checks.LessEqual(10),
                    }
                ),
            ),
        }
    )
    table = pyarrow.Table.from_pydict(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 5.0],
        },
        schema=pyarrow.schema(
            [
                pyarrow.field("a", pyarrow.int64(), nullable=False),
                pyarrow.field("b", pyarrow.float32()),
            ]
        ),
    )
    with pytest.raises(ValueError, match=r"Schema does not match expected schema.+"):
        iudex.validate.validate_pyarrow(table, schema)


def test_validate_pyarrow_cast():
    schema = iudex.schema.Schema(
        {
            iudex.schema.Field(
                "a",
                pyarrow.int64(),
                checks=frozenset(
                    {
                        iudex.checks.Unique(),
                        iudex.checks.Greater(0),
                    }
                ),
            ),
            iudex.schema.Field(
                "b",
                pyarrow.float32(),
                checks=frozenset(
                    {
                        iudex.checks.Greater(0),
                        iudex.checks.LessEqual(10),
                    }
                ),
            ),
        }
    )
    table = pyarrow.Table.from_pydict(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 5.0],
        },
        schema=pyarrow.schema(
            [
                pyarrow.field("a", pyarrow.int64(), nullable=True),
                pyarrow.field("b", pyarrow.float64()),
            ]
        ),
    )
    res = iudex.validate.validate_pyarrow(table, schema, cast=True)
    assert set(res.schema) == set(schema.to_pyarrow())


@pytest.mark.xfail(
    pd.__version__ < "2.2.1",
    reason="Fails due to: https://github.com/pandas-dev/pandas/pull/57173",
)
def test_validate_dataframe():
    schema = iudex.schema.Schema(
        {
            iudex.schema.Field(
                "a",
                pyarrow.int64(),
                nullable=False,
                checks={
                    iudex.checks.Unique(),
                    iudex.checks.Greater(0),
                },
            ),
            iudex.schema.Field(
                "b",
                pyarrow.float32(),
                checks={
                    iudex.checks.Greater(0),
                    iudex.checks.LessEqual(10),
                },
            ),
        }
    )
    dataframe = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 5.0],
        },
    ).astype(
        {
            "a": pd.Int64Dtype(),
            "b": "float32",
        },
    )
    iudex.validate.validate_dataframe(dataframe, schema)
