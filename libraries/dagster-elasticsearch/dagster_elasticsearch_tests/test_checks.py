"""Tests for ``build_indexed_asset_check``.

We materialise an asset that records the same metadata
``ElasticsearchIOManager.handle_output`` would (``indexed`` /
``failures``) and run the generated asset check against the resulting
Dagster instance. No real Elasticsearch involved; the helper only reads
materialisation metadata.
"""

from dagster import (
    AssetCheckEvaluation,
    AssetChecksDefinition,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetsDefinition,
    DagsterInstance,
    Definitions,
    MetadataValue,
    asset,
)

from dagster_elasticsearch import build_indexed_asset_check


def _asset_recording(*, indexed: int, failures: int = 0) -> AssetsDefinition:
    @asset(name="search_docs")
    def search_docs(context: AssetExecutionContext) -> None:
        meta: dict[str, MetadataValue] = {
            "index": MetadataValue.text("docs"),
            "indexed": MetadataValue.int(indexed),
        }
        if failures:
            meta["failures"] = MetadataValue.int(failures)
        context.add_output_metadata(meta)

    return search_docs


def _bare_asset() -> AssetsDefinition:
    @asset(name="bare_asset")
    def bare_asset() -> None:
        return None

    return bare_asset


def _run(
    asset_def: AssetsDefinition, check_def: AssetChecksDefinition
) -> list[AssetCheckEvaluation]:
    """Execute the implicit asset+check job, return list of check evaluations."""
    instance = DagsterInstance.ephemeral()
    defs = Definitions(assets=[asset_def], asset_checks=[check_def])
    job = defs.resolve_implicit_global_asset_job_def()
    res = job.execute_in_process(instance=instance, raise_on_error=False)
    return list(res.get_asset_check_evaluations())


def test_helper_passes_when_indexed_meets_min() -> None:
    asset_def = _asset_recording(indexed=5)
    check_def = build_indexed_asset_check(asset=asset_def, min_indexed=1)

    evals = _run(asset_def, check_def)
    assert len(evals) == 1
    assert evals[0].passed
    assert evals[0].metadata["indexed"].value == 5
    assert evals[0].metadata["failures"].value == 0


def test_helper_fails_when_indexed_below_min() -> None:
    asset_def = _asset_recording(indexed=0)
    check_def = build_indexed_asset_check(asset=asset_def, min_indexed=1)

    evals = _run(asset_def, check_def)
    assert len(evals) == 1
    assert not evals[0].passed
    assert evals[0].metadata["indexed"].value == 0


def test_helper_fails_when_failures_above_max() -> None:
    asset_def = _asset_recording(indexed=10, failures=3)
    check_def = build_indexed_asset_check(asset=asset_def, min_indexed=1, max_failures=2)

    evals = _run(asset_def, check_def)
    assert len(evals) == 1
    assert not evals[0].passed
    assert evals[0].metadata["failures"].value == 3


def test_helper_default_max_failures_zero() -> None:
    """Default ``max_failures=0`` should fail on any failure."""
    asset_def = _asset_recording(indexed=10, failures=1)
    check_def = build_indexed_asset_check(asset=asset_def, min_indexed=1)

    evals = _run(asset_def, check_def)
    assert not evals[0].passed


def test_helper_severity_default_error() -> None:
    asset_def = _asset_recording(indexed=0)
    check_def = build_indexed_asset_check(asset=asset_def, min_indexed=1)

    evals = _run(asset_def, check_def)
    assert evals[0].severity == AssetCheckSeverity.ERROR


def test_helper_severity_overridden_to_warn() -> None:
    asset_def = _asset_recording(indexed=0)
    check_def = build_indexed_asset_check(
        asset=asset_def, min_indexed=1, severity=AssetCheckSeverity.WARN
    )

    evals = _run(asset_def, check_def)
    assert evals[0].severity == AssetCheckSeverity.WARN


def test_helper_handles_missing_metadata() -> None:
    """If ``indexed`` is absent from materialisation metadata, treat as 0."""
    asset_def = _bare_asset()
    check_def = build_indexed_asset_check(asset=asset_def, min_indexed=1)

    evals = _run(asset_def, check_def)
    assert not evals[0].passed
    assert evals[0].metadata["indexed"].value == 0


def test_helper_custom_name() -> None:
    asset_def = _asset_recording(indexed=5)
    check_def = build_indexed_asset_check(asset=asset_def, name="my_custom_check")

    evals = _run(asset_def, check_def)
    assert evals[0].check_name == "my_custom_check"


def test_helper_default_name() -> None:
    asset_def = _asset_recording(indexed=5)
    check_def = build_indexed_asset_check(asset=asset_def)

    evals = _run(asset_def, check_def)
    assert evals[0].check_name == "indexed_count"
