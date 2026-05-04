import datetime
from collections.abc import Iterable
from typing import Any, Literal

from dagster import (
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
)
from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError, bulk, scan
from pydantic import Field

from .config import BaseConnectionConfig, ElasticsearchIndexConfig
from .errors import ElasticsearchBulkIndexError

RolloverStrategy = Literal["auto", "timestamp", "run_id", "partition", "none"]


class ElasticsearchIOManager(ConfigurableIOManager):
    """IO manager that bulk-indexes asset outputs into Elasticsearch.

    Accepts ``list[dict]`` or anything convertible via ``__iter__`` into dicts.
    On read, returns ``list[dict]`` via the scan helper.

    With ``use_alias=True`` the manager writes to a fresh index and atomically
    swaps a stable alias to the new index — readers and downstream assets always
    see a consistent view via the alias name.

    Rollover strategies (when ``use_alias=True``):
        - ``auto``: ``partition`` if asset is partitioned, else ``timestamp``.
        - ``timestamp``: UTC microsecond timestamp suffix, e.g.
          ``my-index-20260504t120000z123456``. Microsecond precision avoids
          collisions for materialisations less than a second apart.
        - ``run_id``: Dagster run id suffix.
        - ``partition``: partition key suffix (errors if asset isn't partitioned).
        - ``none``: no suffix; alias swap still occurs but writes overwrite the
          same physical index. Mostly useful for testing.

    Examples:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_elasticsearch import (
                ElasticsearchIOManager,
                HostsConfig,
            )

            @asset
            def docs() -> list[dict]:
                return [{"_id": "1", "title": "hello"}]

            defs = Definitions(
                assets=[docs],
                resources={
                    "io_manager": ElasticsearchIOManager(
                        connection_config=HostsConfig(hosts=["http://localhost:9200"]),
                        index="docs",
                        use_alias=True,
                        keep_last=3,
                    ),
                },
            )
    """

    connection_config: BaseConnectionConfig = Field(
        description="Connection configuration. Use HostsConfig or CloudConfig.",
    )
    index: str = Field(
        description=(
            "Target index name. When use_alias=True this becomes the alias name "
            "and the physical index gets a rollover suffix."
        ),
    )
    id_field: str | None = Field(
        default="_id",
        description=(
            "Document field used as the Elasticsearch _id. If the field is absent "
            "from a doc, Elasticsearch auto-generates an id."
        ),
    )
    bulk_chunk_size: int = Field(
        default=500,
        description="Number of docs per bulk request.",
    )
    max_chunk_bytes: int | None = Field(
        default=None,
        description=(
            "Maximum bulk request size in bytes. When set, overrides "
            "``bulk_chunk_size`` as the limit if reached first. Useful for "
            "very large documents."
        ),
    )
    fail_fast: bool = Field(
        default=True,
        description=(
            "Raise on the first per-document error during bulk indexing. "
            "When False, errors are collected and logged but do not abort "
            "the materialisation."
        ),
    )
    index_config: ElasticsearchIndexConfig | None = Field(
        default=None,
        description=(
            "Optional mappings and settings applied when the IO manager "
            "creates an index (alias rollover only)."
        ),
    )
    refresh: bool = Field(
        default=True,
        description="Refresh the index after write so docs are immediately searchable.",
    )
    use_alias: bool = Field(
        default=False,
        description="Write to a new index and swap an alias atomically.",
    )
    rollover_strategy: RolloverStrategy = Field(
        default="auto",
        description="How to compute the rollover index suffix when use_alias=True.",
    )
    keep_last: int | None = Field(
        default=None,
        description=(
            "When use_alias=True, retain only the N most recent rollover indices "
            "(matching '{index}-*'). None disables cleanup."
        ),
    )
    request_timeout: float | None = Field(
        default=None,
        description="Client-side per-request timeout in seconds.",
    )
    server_timeout: float | None = Field(
        default=None,
        description=(
            "Server-side timeout in seconds applied to cluster operations "
            "(index creation, bulk indexing, alias updates). Defaults to "
            "the elasticsearch-py default."
        ),
    )
    additional_client_kwargs: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional kwargs forwarded to the Elasticsearch client.",
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def _client(self) -> Elasticsearch:
        kwargs: dict[str, Any] = self.connection_config.to_client_kwargs()
        if self.request_timeout is not None:
            kwargs["request_timeout"] = self.request_timeout
        kwargs.update(self.additional_client_kwargs)
        return Elasticsearch(**kwargs)

    @property
    def _server_timeout_str(self) -> str | None:
        if self.server_timeout is None:
            return None
        return f"{self.server_timeout}s"

    def _resolve_strategy(self, context: OutputContext) -> RolloverStrategy:
        if self.rollover_strategy != "auto":
            return self.rollover_strategy
        if context.has_partition_key:
            return "partition"
        return "timestamp"

    def _rollover_suffix(self, context: OutputContext) -> str:
        strategy = self._resolve_strategy(context)
        if strategy == "timestamp":
            # ES index names must be lowercase, hence lowercase 't'/'z'.
            return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dt%H%M%Sz%f")
        if strategy == "run_id":
            return context.run_id.replace("-", "")
        if strategy == "partition":
            if not context.has_partition_key:
                raise ValueError("rollover_strategy='partition' requires a partitioned asset")
            return _slugify(context.partition_key)
        if strategy == "none":
            return ""
        raise ValueError(f"Unknown rollover strategy: {strategy}")

    def _target_index(self, context: OutputContext) -> str:
        if not self.use_alias:
            if context.has_partition_key:
                return f"{self.index}-{_slugify(context.partition_key)}"
            return self.index
        suffix = self._rollover_suffix(context)
        return self.index if not suffix else f"{self.index}-{suffix}"

    def _read_target(self, context: InputContext) -> str:
        if self.use_alias:
            return self.index
        if context.has_partition_key:
            return f"{self.index}-{_slugify(context.partition_key)}"
        return self.index

    def _swap_alias(self, client: Elasticsearch, alias: str, new_index: str) -> None:
        """Atomically point alias at new_index, removing it from any prior indices."""
        previous: list[str] = []
        if client.indices.exists_alias(name=alias):
            previous = list(client.indices.get_alias(name=alias).body.keys())

        actions: list[dict[str, Any]] = [
            {"remove": {"index": old, "alias": alias}} for old in previous if old != new_index
        ]
        actions.append({"add": {"index": new_index, "alias": alias}})
        client.indices.update_aliases(actions=actions)

    def _cleanup_old(self, client: Elasticsearch, alias: str, current: str) -> None:
        if self.keep_last is None:
            return
        if not client.indices.exists(index=f"{alias}-*"):
            return
        names = sorted(client.indices.get(index=f"{alias}-*").body.keys(), reverse=True)
        keep: set[str] = {current}
        for name in names:
            if name in keep:
                continue
            if len(keep) >= self.keep_last:
                client.indices.delete(index=name, ignore_unavailable=True)
            else:
                keep.add(name)

    def handle_output(self, context: OutputContext, obj: Any) -> None:  # noqa: ANN401
        target = self._target_index(context)
        client = self._client()
        server_timeout = self._server_timeout_str
        try:
            if self.use_alias:
                # Always start from a clean rollover index when suffix is non-empty.
                if target != self.index and client.indices.exists(index=target):
                    client.indices.delete(index=target)
                create_kwargs: dict[str, Any] = {"index": target}
                if self.index_config is not None:
                    if self.index_config.mappings is not None:
                        create_kwargs["mappings"] = self.index_config.mappings
                    if self.index_config.settings is not None:
                        create_kwargs["settings"] = self.index_config.settings
                if server_timeout is not None:
                    create_kwargs["timeout"] = server_timeout
                client.indices.create(**create_kwargs)
                client.cluster.health(index=target, wait_for_status="yellow", timeout="30s")

            # Stream documents through bulk() so memory stays bounded for
            # large LazyFrame/DataFrame inputs.
            actions = (
                _action(target, doc, self.id_field)
                for doc in _iter_docs(obj, chunk_size=self.bulk_chunk_size)
            )
            bulk_kwargs: dict[str, Any] = {
                "client": client,
                "actions": actions,
                "chunk_size": self.bulk_chunk_size,
                "raise_on_error": self.fail_fast,
                "stats_only": True,
            }
            if self.max_chunk_bytes is not None:
                bulk_kwargs["max_chunk_bytes"] = self.max_chunk_bytes
            if server_timeout is not None:
                bulk_kwargs["timeout"] = server_timeout
            try:
                successes, failures = bulk(**bulk_kwargs)
            except BulkIndexError as e:
                raise ElasticsearchBulkIndexError(
                    f"Bulk indexing failed with {len(e.errors)} error(s).",
                    errors=e.errors,
                ) from e

            if self.refresh:
                client.indices.refresh(index=target)

            if self.use_alias:
                self._swap_alias(client, self.index, target)
                self._cleanup_old(client, self.index, target)

            metadata: dict[str, Any] = {
                "index": MetadataValue.text(target),
                "indexed": MetadataValue.int(successes),
            }
            # stats_only=True guarantees `failures` is an int, but the
            # elasticsearch-py type stubs declare it as `int | list`.
            failure_count = failures if isinstance(failures, int) else len(failures)
            if failure_count:
                metadata["failures"] = MetadataValue.int(failure_count)
            if self.use_alias:
                metadata["alias"] = MetadataValue.text(self.index)
            context.add_output_metadata(metadata)
        finally:
            client.close()

    def load_input(self, context: InputContext) -> list[dict]:
        target = self._read_target(context)
        client = self._client()
        try:
            return [
                hit["_source"]
                for hit in scan(client, index=target, query={"query": {"match_all": {}}})
            ]
        finally:
            client.close()


def _slugify(value: str) -> str:
    return "".join(c if c.isalnum() else "-" for c in value.lower()).strip("-")


def _iter_docs(obj: Any, chunk_size: int) -> Iterable[dict]:  # noqa: ANN401
    """Yield documents from supported inputs without materialising the whole set.

    Polars LazyFrames are streamed via ``collect(streaming=True)`` and sliced
    in ``chunk_size`` rows so very large parquet-backed datasets don't have to
    fit in memory at once. Polars DataFrames and pandas DataFrames are sliced
    similarly. ``list``/``dict`` inputs pass through unchanged.
    """
    if obj is None:
        return
    if isinstance(obj, dict):
        yield obj
        return
    if isinstance(obj, list):
        yield from obj
        return

    # Detect by class + module name to avoid unconditional pandas/polars imports.
    type_name = type(obj).__name__
    module_name = type(obj).__module__.split(".")[0]

    if type_name == "DataFrame" and module_name == "pandas":
        for start in range(0, len(obj), chunk_size):
            chunk = obj.iloc[start : start + chunk_size]
            yield from chunk.to_dict(orient="records")
        return

    if module_name == "polars" and type_name == "LazyFrame":
        # Stream the LazyFrame so we don't materialise the full result.
        # Polars 1.25 renamed ``streaming=True`` → ``engine="streaming"``.
        try:
            frame = obj.collect(engine="streaming")
        except TypeError:
            try:
                frame = obj.collect(streaming=True)  # type: ignore[call-arg]
            except TypeError:
                frame = obj.collect()
        type_name = "DataFrame"
        obj = frame

    if module_name == "polars" and type_name == "DataFrame":
        for slice_ in obj.iter_slices(n_rows=chunk_size):
            yield from slice_.to_dicts()
        return

    raise TypeError(
        f"ElasticsearchIOManager cannot serialise {type(obj).__name__}; "
        "supply a dict, list[dict], pandas DataFrame, or polars DataFrame/LazyFrame."
    )


def _to_docs(obj: Any) -> Iterable[dict]:  # noqa: ANN401
    """Backwards-compatible eager wrapper used by unit tests."""
    return list(_iter_docs(obj, chunk_size=1000))


def _action(index: str, doc: dict, id_field: str | None) -> dict:
    action: dict[str, Any] = {"_index": index, "_source": dict(doc)}
    if id_field and id_field in action["_source"]:
        action["_id"] = action["_source"].pop(id_field)
    return action
