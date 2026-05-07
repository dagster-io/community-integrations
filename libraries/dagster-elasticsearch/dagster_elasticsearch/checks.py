"""Asset check helpers for ``ElasticsearchIOManager`` outputs.

The IO manager records ``indexed``, ``failures``, ``index``, and ``alias`` on
each materialisation's output metadata. These helpers wrap the boilerplate of
reading those values back from the latest materialisation event so a project
can drop in a one-liner asset check.
"""

from collections.abc import Sequence

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    AssetKey,
    AssetsDefinition,
    SourceAsset,
    asset_check,
)


def _read_indexed_metadata(
    context: AssetCheckExecutionContext, asset_key: AssetKey
) -> tuple[int, int]:
    """Pull (indexed, failures) from the latest materialisation of ``asset_key``."""
    record = context.instance.get_latest_materialization_event(asset_key)
    metadata = (
        record.asset_materialization.metadata if record and record.asset_materialization else {}
    )

    def _as_int(key: str) -> int:
        if key not in metadata:
            return 0
        raw = metadata[key].value
        try:
            return int(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return 0

    return _as_int("indexed"), _as_int("failures")


def build_indexed_asset_check(
    *,
    asset: AssetsDefinition | SourceAsset | AssetKey | Sequence[str] | str,
    min_indexed: int = 1,
    max_failures: int = 0,
    name: str = "indexed_count",
    severity: AssetCheckSeverity = AssetCheckSeverity.ERROR,
    blocking: bool = False,
) -> AssetChecksDefinition:
    """Build an asset check that asserts the latest materialisation indexed
    at least ``min_indexed`` documents and saw no more than ``max_failures``.

    The check reads ``indexed`` and ``failures`` from the asset's most recent
    materialisation metadata (recorded by ``ElasticsearchIOManager``).

    :param asset: Asset (definition, source asset, key, or string key) to check.
    :param min_indexed: Minimum acceptable indexed count. Defaults to 1.
    :param max_failures: Maximum acceptable failure count. Defaults to 0.
    :param name: Asset check name. Defaults to ``"indexed_count"``.
    :param severity: Severity when the check fails. Defaults to ``ERROR``.
    :param blocking: When True, downstream assets won't materialise if the
        check fails. Defaults to False.
    :return: An ``AssetChecksDefinition`` suitable for inclusion in a
        ``Definitions`` block.

    Example:
    ```python
    from dagster import Definitions, asset
    from dagster_elasticsearch import (
        ElasticsearchIOManager,
        build_indexed_asset_check,
    )

    @asset(io_manager_key="es_io_manager")
    def search_docs() -> list[dict]:
        return [{"_id": "1", "title": "hello"}]

    defs = Definitions(
        assets=[search_docs],
        asset_checks=[
            build_indexed_asset_check(asset=search_docs, min_indexed=1)
        ],
        resources={"es_io_manager": ElasticsearchIOManager(...)},
    )
    ```
    """

    @asset_check(
        asset=asset,
        name=name,
        blocking=blocking,
    )
    def _indexed_count_check(
        context: AssetCheckExecutionContext,
    ) -> AssetCheckResult:
        asset_key = next(iter(context.check_specs)).asset_key
        indexed, failures = _read_indexed_metadata(context, asset_key)
        passed = indexed >= min_indexed and failures <= max_failures
        return AssetCheckResult(
            passed=passed,
            severity=severity,
            metadata={
                "indexed": indexed,
                "failures": failures,
                "min_indexed": min_indexed,
                "max_failures": max_failures,
            },
            description=(
                f"indexed={indexed} (min {min_indexed}), failures={failures} (max {max_failures})"
            ),
        )

    return _indexed_count_check
