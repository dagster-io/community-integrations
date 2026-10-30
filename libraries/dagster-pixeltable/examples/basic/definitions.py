import json
from pathlib import Path
from typing import Any

import dagster as dg

from dagster_pixeltable import PixeltableResource

TABLE_PATH = "dagster_demo.documents"


class IngestConfig(dg.Config):
    records_path: str = str(Path(__file__).parent / "documents.json")


def validate_records(records: list[dict[str, Any]]) -> None:
    seen: set[str] = set()
    for i, record in enumerate(records):
        source_id = record.get("source_id")
        if not source_id:
            raise ValueError(f"Record {i} has no source_id")
        if source_id in seen:
            raise ValueError(f"Duplicate source_id {source_id!r}")
        seen.add(source_id)


@dg.asset
def ingest_documents(
    config: IngestConfig, pixeltable: PixeltableResource
) -> dg.MaterializeResult:
    table = pixeltable.get_table(TABLE_PATH)
    records = json.loads(Path(config.records_path).read_text())
    validate_records(records)

    num_rows = num_excs = 0
    if records:
        # a replayed source_id updates its row instead of adding one
        status = table.batch_update(records, if_not_exists="insert")
        num_rows, num_excs = status.num_rows, status.num_excs

    md = table.get_metadata()
    return dg.MaterializeResult(
        metadata={
            "dagster/table_name": TABLE_PATH,
            "dagster/row_count": table.count(),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn(name, col["type_"], description=col["computed_with"])
                    for name, col in md["columns"].items()
                ]
            ),
            "pixeltable/table_id": str(md["id"]),
            "pixeltable/version": md["version"],
            "pixeltable/num_rows": num_rows,
            "pixeltable/num_excs": num_excs,
        }
    )


@dg.asset_check(asset=ingest_documents)
def documents_computed(pixeltable: PixeltableResource) -> dg.AssetCheckResult:
    table = pixeltable.get_table(TABLE_PATH)
    rows = table.select(
        table.source_id,
        table.body,
        table.page_count,
        table.body_upper,
        table.chars_per_page,
        error=table.chars_per_page.errortype,
    ).collect()
    errored = [row["source_id"] for row in rows if row["error"] is not None]
    wrong = [
        row["source_id"]
        for row in rows
        if row["error"] is None
        and (
            row["body_upper"] != row["body"].upper()
            or row["chars_per_page"] != len(row["body"]) / row["page_count"]
        )
    ]
    return dg.AssetCheckResult(
        passed=not errored and not wrong,
        metadata={"errored_source_ids": errored, "wrong_source_ids": wrong},
    )


defs = dg.Definitions(
    assets=[ingest_documents],
    asset_checks=[documents_computed],
    resources={
        "pixeltable": PixeltableResource(expected_home=dg.EnvVar("PIXELTABLE_HOME"))
    },
    executor=dg.in_process_executor,
)
