import pyarrow
import pandas as pd
import pytest

import iudex.checks
import iudex.schema
import iudex.validate


def test_validate_pyarrow():
    schema = iudex.schema.Schema(
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
    )
    table = pyarrow.Table.from_pydict(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 5.0],
        },
        schema=schema.to_arrow(),
    )
    iudex.validate.validate_pyarrow(table, schema)


@pytest.mark.xfail(
    pd.__version__ < "2.2.1",
    reason="Fails due to: https://github.com/pandas-dev/pandas/pull/57173",
)
def test_validate_dataframe():
    schema = iudex.schema.Schema(
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
