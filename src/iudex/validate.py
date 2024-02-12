from typing import TypeVar

import pyarrow
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
    if not data.schema.equals(schema.to_arrow()):
        raise ValueError(
            f"Schema does not match expected schema.\n"
            f"Schema: {data.schema!r}.\n"
            f"Expected schema: {schema.to_arrow()!r}.",
        )

    if isinstance(data, pyarrow.dataset.Dataset):
        fields_with_checks = [field.name for field in schema.fields if field.checks]
        data = data.to_table(
            columns=fields_with_checks,
        )

    for field in schema.fields:
        for check in field.checks:
            if not check(data.column(field.name)):
                raise ValueError(
                    f"Check failed for field {field.name!r}.",
                )

    return data
