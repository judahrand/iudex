import pandas as pd
import pyarrow
import pytest

import iudex.checks
import iudex.schema
import iudex.validate
import iudex.errors


def test_validate_pyarrow_pass():
    schema = iudex.schema.Schema(
        {
            iudex.schema.Field(
                "a",
                pyarrow.int64(),
                nullable=False,
                check=iudex.checks.Less(1000) & iudex.checks.Greater(0),
            ),
            iudex.schema.Field(
                "b",
                pyarrow.float32(),
                check=iudex.checks.Greater(0) & iudex.checks.LessEqual(10),
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
                check=iudex.checks.Less(1000) & iudex.checks.Greater(0),
            ),
            iudex.schema.Field(
                "b",
                pyarrow.float64(),
                check=iudex.checks.Greater(0) & iudex.checks.LessEqual(10),
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
    with pytest.raises(
        iudex.errors.SchemaError, match=r"Schema does not match expected schema.+"
    ):
        iudex.validate.validate_pyarrow(table, schema)


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
                check=iudex.checks.Less(1000) & iudex.checks.Greater(0),
            ),
            iudex.schema.Field(
                "b",
                pyarrow.float32(),
                check=iudex.checks.Greater(0) & iudex.checks.LessEqual(10),
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
