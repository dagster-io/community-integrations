from dagster import __version__
from packaging import version

from dagster_iceberg._utils.io import table_writer as table_writer
from dagster_iceberg._utils.partitions import (
    DagsterPartitionToDaftSqlPredicateMapper as DagsterPartitionToDaftSqlPredicateMapper,
)
from dagster_iceberg._utils.partitions import (
    DagsterPartitionToIcebergExpressionMapper as DagsterPartitionToIcebergExpressionMapper,
)
from dagster_iceberg._utils.partitions import (
    DagsterPartitionToPolarsSqlPredicateMapper as DagsterPartitionToPolarsSqlPredicateMapper,
)


def preview(wrapped=None):
    if version.parse(__version__) >= version.parse("1.10.0"):
        from dagster._annotations import (
            preview as decorator,  # pyright: ignore[reportAttributeAccessIssue]
        )
    else:
        from dagster._annotations import (
            experimental as decorator,  # pyright: ignore[reportAttributeAccessIssue]
        )

    if wrapped is not None:
        return decorator(wrapped)

    return decorator
