from typing import TypeVar

import pyarrow
import pyarrow.compute
import pyarrow.dataset
import pyarrow.interchange

from .dataframe_protocol import DataFrame
from .schema import Schema

_ArrowT = TypeVar(
    "_ArrowT",
    pyarrow.Table,
    pyarrow.RecordBatch,
    pyarrow.dataset.Dataset,
)


def validate_dataframe(
    dataframe: DataFrame,
    schema: Schema,
    allow_copy: bool = True,
) -> None:
    """Validate a DataFrame against a schema."""
    table = pyarrow.interchange.from_dataframe(
        dataframe,
        allow_copy=allow_copy,
    )
    validate_pyarrow(table, schema)


def validate_pyarrow(
    data: _ArrowT,
    schema: Schema,
) -> _ArrowT:
    """Validate a Table against a schema."""
    target_schema = schema.to_pyarrow()

    if data.schema != target_schema:
        raise ValueError(
            f"Schema does not match expected schema.\n"
            f"Schema: {data.schema!r}.\n"
            f"Expected schema: {target_schema!r}.",
        )

    for field in schema.fields:
        if field.check is not None:
            if not pyarrow.compute.max(field.check(data[field.name])).as_py():
                raise ValueError(
                    f"Check failed for field {field.name!r}.",
                )

    return data
