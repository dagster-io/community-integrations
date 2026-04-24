# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from dagster import (
    AssetCheckEvaluation,
    AssetCheckSeverity,
    AssetKey,
    TableColumn,
    TableColumnDep,
    TableColumnLineage,
    TableSchema,
)

from dagster_openlineage.facets import (
    DAGSTER_ASSET_CHECK_FACET_SCHEMA_URL,
    build_column_lineage_facet,
    build_dagster_asset_check_run_facet,
    build_data_quality_assertions_facet,
    build_datasource_facet,
    build_error_message_facet,
    build_nominal_time_facet,
    build_schema_facet,
)
from dagster_openlineage.version import __version__ as OPENLINEAGE_DAGSTER_VERSION


def test_schema_facet_happy_path():
    facet = build_schema_facet(
        TableSchema(columns=[TableColumn("x", "int", description="d")])
    )
    assert facet.fields is not None
    assert len(facet.fields) == 1
    assert facet.fields[0].name == "x"
    assert facet.fields[0].type == "int"
    assert facet.fields[0].description == "d"


def test_column_lineage_facet_happy_path():
    lineage = TableColumnLineage(
        deps_by_column={
            "out": [TableColumnDep(asset_key=AssetKey("in"), column_name="src")]
        }
    )
    facet = build_column_lineage_facet(lineage, upstream_namespace="dagster")
    assert "out" in facet.fields
    inputs = facet.fields["out"].inputFields
    assert len(inputs) == 1
    assert inputs[0].namespace == "dagster"
    assert inputs[0].name == "in"
    assert inputs[0].field == "src"


def test_datasource_facet_returns_none_when_empty():
    assert build_datasource_facet() is None


def test_datasource_facet_with_uri():
    facet = build_datasource_facet(name="s3", uri="s3://bucket/key")
    assert facet is not None
    assert facet.name == "s3"
    assert facet.uri == "s3://bucket/key"


def test_nominal_time_none_when_no_partition():
    assert build_nominal_time_facet(None) is None


def test_nominal_time_iso_date_24h_window():
    facet = build_nominal_time_facet("2026-04-23")
    assert facet is not None
    assert facet.nominalStartTime == "2026-04-23T00:00:00.000000Z"
    assert facet.nominalEndTime == "2026-04-24T00:00:00.000000Z"


def test_nominal_time_hour_window_dash_separator():
    facet = build_nominal_time_facet("2026-04-23-15:00")
    assert facet is not None
    assert facet.nominalStartTime == "2026-04-23T15:00:00.000000Z"
    assert facet.nominalEndTime == "2026-04-23T16:00:00.000000Z"


def test_nominal_time_hour_window_t_separator():
    facet = build_nominal_time_facet("2026-04-23T15:00")
    assert facet is not None
    assert facet.nominalStartTime == "2026-04-23T15:00:00.000000Z"


def test_nominal_time_hour_window_with_seconds():
    facet = build_nominal_time_facet("2026-04-23T15:30:45")
    assert facet is not None
    # Minutes/seconds are floored to the hour (heuristic precision).
    assert facet.nominalStartTime == "2026-04-23T15:00:00.000000Z"


def test_nominal_time_static_partition_omitted():
    assert build_nominal_time_facet("us-east-1") is None


def test_nominal_time_multi_partition_composite_omitted():
    assert build_nominal_time_facet("2026-04-23|customer-42") is None


def test_nominal_time_year_only_omitted():
    assert build_nominal_time_facet("2026") is None


def test_nominal_time_compact_format_omitted():
    # YYYYMMDD is ambiguous — don't guess.
    assert build_nominal_time_facet("20260423") is None


def _evaluation(
    *,
    passed: bool = True,
    severity: AssetCheckSeverity = AssetCheckSeverity.ERROR,
    check_name: str = "orders_nonempty",
    metadata=None,
):
    return AssetCheckEvaluation(
        asset_key=AssetKey(["orders"]),
        check_name=check_name,
        passed=passed,
        metadata=metadata,
        severity=severity,
    )


def test_data_quality_assertion_happy_path_passed_error_severity():
    facet = build_data_quality_assertions_facet([_evaluation(passed=True)])
    assert len(facet.assertions) == 1
    assert facet.assertions[0].assertion == "orders_nonempty"
    assert facet.assertions[0].success is True


def test_data_quality_assertion_failed_error_severity():
    facet = build_data_quality_assertions_facet([_evaluation(passed=False)])
    assert facet.assertions[0].success is False


def test_data_quality_assertion_warn_default_maps_to_success():
    facet = build_data_quality_assertions_facet(
        [_evaluation(passed=True, severity=AssetCheckSeverity.WARN)]
    )
    assert facet.assertions[0].success is True


def test_data_quality_assertion_warn_strict_maps_to_failure():
    facet = build_data_quality_assertions_facet(
        [_evaluation(passed=True, severity=AssetCheckSeverity.WARN)],
        strict_assertion_mapping=True,
    )
    assert facet.assertions[0].success is False


def test_data_quality_assertion_multiple_checks_on_same_asset():
    # R16: one facet per asset, multiple Assertion entries.
    facet = build_data_quality_assertions_facet(
        [
            _evaluation(check_name="a", passed=True),
            _evaluation(check_name="b", passed=False),
        ]
    )
    assert [a.assertion for a in facet.assertions] == ["a", "b"]
    assert [a.success for a in facet.assertions] == [True, False]


def test_data_quality_assertion_extracts_column_from_metadata():
    facet = build_data_quality_assertions_facet(
        [_evaluation(metadata={"column": "email"})]
    )
    assert facet.assertions[0].column == "email"


def test_data_quality_assertion_uses_dagster_column_key():
    facet = build_data_quality_assertions_facet(
        [_evaluation(metadata={"dagster/column": "email"})]
    )
    assert facet.assertions[0].column == "email"


def test_error_message_facet():
    facet = build_error_message_facet(message="boom", stack_trace="Traceback...")
    assert facet.message == "boom"
    assert facet.programmingLanguage == "python"
    assert facet.stackTrace == "Traceback..."


def test_dagster_asset_check_run_facet_contains_schema_url():
    facet = build_dagster_asset_check_run_facet([_evaluation()])
    assert "dagster_asset_check" in facet
    payload = facet["dagster_asset_check"]
    assert payload["_schemaURL"] == DAGSTER_ASSET_CHECK_FACET_SCHEMA_URL
    assert OPENLINEAGE_DAGSTER_VERSION in payload["_schemaURL"]
    assert payload["checks"][0]["assertion"] == "orders_nonempty"
    assert payload["checks"][0]["severity"] == "ERROR"


def test_dagster_asset_check_run_facet_warn_severity():
    facet = build_dagster_asset_check_run_facet(
        [_evaluation(severity=AssetCheckSeverity.WARN)]
    )
    assert facet["dagster_asset_check"]["checks"][0]["severity"] == "WARN"
