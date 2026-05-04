"""Unit tests for IO manager helpers — no Elasticsearch required."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from dagster_elasticsearch.io_manager import (
    _action,
    _slugify,
    _to_docs,
)


class TestSlugify:
    def test_lowercase(self) -> None:
        assert _slugify("ABC") == "abc"

    def test_replaces_non_alnum_with_dash(self) -> None:
        assert _slugify("hello world!") == "hello-world"

    def test_strips_leading_trailing_dashes(self) -> None:
        assert _slugify("___foo___") == "foo"

    def test_partition_dates(self) -> None:
        assert _slugify("2026-05-04") == "2026-05-04"


class TestToDocs:
    def test_none(self) -> None:
        assert list(_to_docs(None)) == []

    def test_dict(self) -> None:
        assert list(_to_docs({"a": 1})) == [{"a": 1}]

    def test_list(self) -> None:
        docs = [{"a": 1}, {"b": 2}]
        assert list(_to_docs(docs)) == docs

    def test_dataframe(self) -> None:
        df = pd.DataFrame([{"a": 1}, {"a": 2}])
        result = list(_to_docs(df))
        assert result == [{"a": 1}, {"a": 2}]

    def test_unsupported_type(self) -> None:
        with pytest.raises(TypeError, match="cannot serialise"):
            list(_to_docs(42))

    def test_polars_dataframe(self) -> None:
        pl = pytest.importorskip("polars")
        df = pl.DataFrame([{"a": 1}, {"a": 2}])
        assert list(_to_docs(df)) == [{"a": 1}, {"a": 2}]

    def test_polars_lazyframe(self) -> None:
        pl = pytest.importorskip("polars")
        lf = pl.DataFrame([{"a": 1}, {"a": 2}]).lazy()
        assert list(_to_docs(lf)) == [{"a": 1}, {"a": 2}]

    def test_pyarrow_table(self) -> None:
        pa = pytest.importorskip("pyarrow")
        table = pa.Table.from_pylist([{"a": 1}, {"a": 2}])
        assert list(_to_docs(table)) == [{"a": 1}, {"a": 2}]

    def test_pyarrow_record_batch_reader(self) -> None:
        pa = pytest.importorskip("pyarrow")
        batches = [pa.RecordBatch.from_pylist([{"a": 1}]), pa.RecordBatch.from_pylist([{"a": 2}])]
        reader = pa.RecordBatchReader.from_batches(batches[0].schema, iter(batches))
        assert list(_to_docs(reader)) == [{"a": 1}, {"a": 2}]

    def test_generator(self) -> None:
        def gen() -> object:
            yield {"a": 1}
            yield {"a": 2}

        assert list(_to_docs(gen())) == [{"a": 1}, {"a": 2}]

    def test_pydantic_model_list(self) -> None:
        from pydantic import BaseModel

        class Item(BaseModel):
            id: int
            name: str

        items = [Item(id=1, name="a"), Item(id=2, name="b")]
        assert list(_to_docs(items)) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    def test_dataclass_list(self) -> None:
        from dataclasses import dataclass

        @dataclass
        class Item:
            id: int
            name: str

        items = [Item(id=1, name="a")]
        assert list(_to_docs(items)) == [{"id": 1, "name": "a"}]


class TestAction:
    def test_no_id_field(self) -> None:
        action = _action("idx", {"a": 1}, id_field=None)
        assert action == {"_index": "idx", "_source": {"a": 1}}

    def test_id_field_present(self) -> None:
        action = _action("idx", {"_id": "42", "a": 1}, id_field="_id")
        assert action == {"_index": "idx", "_id": "42", "_source": {"a": 1}}

    def test_id_field_absent(self) -> None:
        action = _action("idx", {"a": 1}, id_field="_id")
        assert action == {"_index": "idx", "_source": {"a": 1}}

    def test_does_not_mutate_input(self) -> None:
        doc = {"_id": "x", "a": 1}
        _action("idx", doc, id_field="_id")
        assert doc == {"_id": "x", "a": 1}


class TestTargetIndex:
    def _io(self, **kwargs: object) -> object:
        from dagster_elasticsearch import ElasticsearchIOManager, HostsConfig

        return ElasticsearchIOManager(
            connection_config=HostsConfig(hosts=["http://localhost:9200"]),
            index="docs",
            **kwargs,  # type: ignore[arg-type]
        )

    def _ctx(
        self,
        partition_key: str | None = None,
        run_id: str = "abc-def",
        definition_metadata: dict | None = None,
        output_metadata: dict | None = None,
    ) -> MagicMock:
        ctx = MagicMock()
        ctx.has_partition_key = partition_key is not None
        ctx.partition_key = partition_key
        ctx.run_id = run_id
        ctx.definition_metadata = definition_metadata or {}
        ctx.output_metadata = output_metadata or {}
        return ctx

    def test_no_alias_no_partition(self) -> None:
        io = self._io()
        assert io._target_index(self._ctx(), "docs") == "docs"  # type: ignore[attr-defined]

    def test_no_alias_with_partition(self) -> None:
        io = self._io()
        assert io._target_index(self._ctx("p1"), "docs") == "docs-p1"  # type: ignore[attr-defined]

    def test_alias_run_id(self) -> None:
        io = self._io(use_alias=True, rollover_strategy="run_id")
        assert io._target_index(self._ctx(run_id="abc-def"), "docs") == "docs-abcdef"  # type: ignore[attr-defined]

    def test_alias_partition_strategy_unpartitioned_raises(self) -> None:
        io = self._io(use_alias=True, rollover_strategy="partition")
        with pytest.raises(ValueError, match="partitioned asset"):
            io._target_index(self._ctx(), "docs")  # type: ignore[attr-defined]

    def test_alias_none_strategy(self) -> None:
        io = self._io(use_alias=True, rollover_strategy="none")
        assert io._target_index(self._ctx(), "docs") == "docs"  # type: ignore[attr-defined]

    def test_auto_resolves_to_partition_when_partitioned(self) -> None:
        io = self._io(use_alias=True, rollover_strategy="auto")
        assert io._target_index(self._ctx("p1"), "docs") == "docs-p1"  # type: ignore[attr-defined]

    def test_auto_resolves_to_timestamp_when_unpartitioned(self) -> None:
        io = self._io(use_alias=True, rollover_strategy="auto")
        target = io._target_index(self._ctx(), "docs")  # type: ignore[attr-defined]
        # Timestamp suffix is lowercase.
        assert target.startswith("docs-") and target.lower() == target

    def test_definition_metadata_overrides_strategy(self) -> None:
        io = self._io(use_alias=True, rollover_strategy="timestamp")
        ctx = self._ctx(definition_metadata={"rollover_strategy": "run_id"})
        target = io._target_index(ctx, "docs")  # type: ignore[attr-defined]
        assert target == "docs-abcdef"

    def test_output_metadata_beats_definition_metadata(self) -> None:
        io = self._io(use_alias=True, rollover_strategy="timestamp")
        ctx = self._ctx(
            definition_metadata={"rollover_strategy": "run_id"},
            output_metadata={"rollover_strategy": "none"},
        )
        target = io._target_index(ctx, "docs")  # type: ignore[attr-defined]
        assert target == "docs"


class TestBulkIndexError:
    def test_exposes_errors(self) -> None:
        from dagster_elasticsearch import ElasticsearchBulkIndexError

        errors = [{"index": {"error": {"type": "mapper_parsing_exception"}}}]
        e = ElasticsearchBulkIndexError("boom", errors=errors)
        assert e.errors == errors
        assert "boom" in str(e)
