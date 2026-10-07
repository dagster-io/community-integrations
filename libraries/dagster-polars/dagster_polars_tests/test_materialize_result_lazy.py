import polars as pl
import polars.testing as pl_testing
from dagster import MaterializeResult, asset, materialize

from dagster_polars import PolarsParquetIOManager


def test_lazy_frame_in_materialize_result(
    polars_parquet_io_manager: PolarsParquetIOManager,
):
    df = pl.DataFrame({"a": [1, 2, 3]})

    @asset(io_manager_def=polars_parquet_io_manager)
    def upstream() -> MaterializeResult:
        return MaterializeResult(value=df.lazy(), metadata={"rows": 3})

    @asset(io_manager_def=polars_parquet_io_manager)
    def downstream(upstream: pl.LazyFrame) -> pl.DataFrame:
        return upstream.collect()

    result = materialize([upstream, downstream])
    assert result.success
    pl_testing.assert_frame_equal(result.output_for_node("downstream"), df)
