# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import pytest

from dagster_openlineage.naming import (
    NamespaceTemplateError,
    parse_namespace_template,
    resolve_namespace,
)


def test_resolve_default_token():
    parsed = parse_namespace_template("{namespace}")
    assert resolve_namespace(parsed, "dagster", {}) == "dagster"


def test_resolve_tag_token():
    parsed = parse_namespace_template("{namespace}/{tag:tenant}")
    assert resolve_namespace(parsed, "dagster", {"tenant": "acme"}) == "dagster/acme"


def test_resolve_unresolved_tag_collapses_trailing_slash():
    parsed = parse_namespace_template("{namespace}/{tag:tenant}")
    assert resolve_namespace(parsed, "dagster", {}) == "dagster"


def test_resolve_collapses_consecutive_slashes():
    parsed = parse_namespace_template("{namespace}//{tag:tenant}")
    assert resolve_namespace(parsed, "dagster", {"tenant": "acme"}) == "dagster/acme"


def test_resolve_all_tokens_unresolved_yields_empty():
    parsed = parse_namespace_template("{tag:tenant}/{tag:region}")
    assert resolve_namespace(parsed, "dagster", {}) == ""


def test_resolve_multiple_tags():
    parsed = parse_namespace_template("{namespace}/{tag:tenant}/{tag:region}")
    result = resolve_namespace(
        parsed, "dagster", {"tenant": "acme", "region": "us-east"}
    )
    assert result == "dagster/acme/us-east"


def test_resolve_literal_preserved():
    parsed = parse_namespace_template("prod/{namespace}/{tag:tenant}")
    assert (
        resolve_namespace(parsed, "dagster", {"tenant": "acme"}) == "prod/dagster/acme"
    )


def test_parse_rejects_unknown_token():
    with pytest.raises(NamespaceTemplateError) as exc_info:
        parse_namespace_template("{namespace}/{unknown_token}")
    assert "unknown_token" in str(exc_info.value)


def test_parse_rejects_v03_code_location_token():
    # {code_location} is deferred to v0.3 — the error message must name the deferral.
    with pytest.raises(NamespaceTemplateError) as exc_info:
        parse_namespace_template("{namespace}/{code_location}")
    msg = str(exc_info.value)
    assert "code_location" in msg
    assert "v0.3" in msg or "0.3" in msg


def test_parse_rejects_v03_repository_token():
    with pytest.raises(NamespaceTemplateError) as exc_info:
        parse_namespace_template("{namespace}/{repository}")
    assert "repository" in str(exc_info.value)


def test_parse_empty_tag_key_rejected():
    with pytest.raises(NamespaceTemplateError):
        parse_namespace_template("{namespace}/{tag:}")


def test_resolve_empty_template():
    parsed = parse_namespace_template("")
    assert resolve_namespace(parsed, "dagster", {}) == ""
