from typing import TypeVar

import pyarrow
import pyarrow.dataset
import pyarrow.interchange
import ibis

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
    cast: bool = False,
) -> _ArrowT:
    """Validate a Table against a schema."""
    target_schema = schema.to_ibis()
    table = ibis.memtable(data)

    if cast:
        table = table.cast(target_schema)

    if not set(table.schema().items()) == set(target_schema.items()):
        raise ValueError(
            f"Schema does not match expected schema.\n"
            f"Schema: {table.schema()!r}.\n"
            f"Expected schema: {target_schema!r}.",
        )

    for field in schema.fields:
        for check in field.checks:
            if not check(table[field.name]):
                raise ValueError(
                    f"Check failed for field {field.name!r}.",
                )

    return table.to_pyarrow()
