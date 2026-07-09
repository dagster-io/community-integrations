# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time
from unittest.mock import MagicMock

from dagster import AssetKey
from openlineage.client.uuid import generate_new_uuid

from dagster_openlineage.adapter import OpenLineageAdapter
from dagster_openlineage.emitter import OpenLineageEmitter


def _rid() -> str:
    return str(generate_new_uuid())


def _make_adapter(**kwargs) -> tuple[OpenLineageAdapter, MagicMock]:
    client = MagicMock()
    adapter = OpenLineageAdapter(emitter=OpenLineageEmitter(client=client), **kwargs)
    return adapter, client


def test_namespace_template_splits_by_run_tag():
    adapter, client = _make_adapter(
        namespace="dagster",
        namespace_template="{namespace}/{tag:tenant}",
    )
    adapter.asset_materialization(
        AssetKey(["orders"]),
        run_id=_rid(),
        timestamp=time.time(),
        run_tags={"tenant": "acme"},
    )
    adapter.asset_materialization(
        AssetKey(["orders"]),
        run_id=_rid(),
        timestamp=time.time(),
        run_tags={"tenant": "globex"},
    )
    assert client.emit.call_count == 2
    namespaces = [c.args[0].job.namespace for c in client.emit.call_args_list]
    assert namespaces == ["dagster/acme", "dagster/globex"]


def test_namespace_template_falls_back_when_tag_missing():
    # Template resolves to "dagster" after trailing-slash strip when the tag is
    # not present; namespace still ends up as the configured default.
    adapter, client = _make_adapter(
        namespace="dagster",
        namespace_template="{namespace}/{tag:tenant}",
    )
    adapter.asset_materialization(
        AssetKey(["orders"]),
        run_id=_rid(),
        timestamp=time.time(),
        run_tags={},
    )
    assert client.emit.call_args.args[0].job.namespace == "dagster"


def test_no_template_uses_configured_namespace():
    adapter, client = _make_adapter(namespace="prod")
    adapter.asset_materialization(
        AssetKey(["orders"]), run_id=_rid(), timestamp=time.time()
    )
    assert client.emit.call_args.args[0].job.namespace == "prod"
