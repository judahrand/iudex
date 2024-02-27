from typing import TypeVar

import ibis
import ibis.expr.types
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
    cast: bool = False,
) -> _ArrowT:
    """Validate a Table against a schema."""
    if cast:
        target_schema = schema.to_pyarrow()
        data = (
            data.select(
                target_schema.names,
            )
            .cast(
                target_schema,
            )
            .select(
                data.schema.names,
            )
        )

    validate_ibis(ibis.memtable(data), schema, cast=False)

    return data


def validate_ibis(
    data: ibis.expr.types.TableExpr,
    schema: Schema,
    cast: bool = False,
) -> ibis.expr.types.TableExpr:
    """Validate an Ibis table against a schema."""
    target_schema = schema.to_ibis()

    if cast:
        data = data.cast(
            target_schema,
        ).select(
            target_schema.names,
        )

    if not set(data.schema().items()) == set(target_schema.items()):
        raise ValueError(
            f"Schema does not match expected schema.\n"
            f"Schema: {data.schema()!r}.\n"
            f"Expected schema: {target_schema!r}.",
        )

    for field in schema.fields:
        if field.check is not None:
            if not pyarrow.compute.max(field.check(data[field.name])).as_py():
                raise ValueError(
                    f"Check failed for field {field.name!r}.",
                )

    return data
