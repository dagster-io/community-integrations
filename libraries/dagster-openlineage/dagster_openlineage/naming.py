# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Namespace template parsing and per-event resolution.

v0.2 supports two token families:
  * ``{namespace}`` — the configured default namespace
  * ``{tag:KEY}`` — a Dagster run tag

Unknown tokens (including ``{code_location}`` and ``{repository}``, which are
deferred to v0.3) raise ``NamespaceTemplateError`` at parse time so misconfig
surfaces at construction, not per-event. Unresolved tag values collapse to the
empty string; consecutive slashes collapse; trailing slashes strip.
"""

from __future__ import annotations

import re
from typing import List, Mapping, Tuple

SUPPORTED_LITERAL_TOKENS = frozenset({"namespace"})
_V03_DEFERRED_TOKENS = frozenset({"code_location", "repository"})
_TOKEN_RE = re.compile(r"\{([^}]*)\}")


class NamespaceTemplateError(ValueError):
    """Raised when a namespace template contains an unsupported token."""


class _Segment:
    __slots__ = ()


class _Literal(_Segment):
    __slots__ = ("text",)

    def __init__(self, text: str) -> None:
        self.text = text


class _NamespaceToken(_Segment):
    __slots__ = ()


class _TagToken(_Segment):
    __slots__ = ("key",)

    def __init__(self, key: str) -> None:
        self.key = key


ParsedTemplate = Tuple[_Segment, ...]


def parse_namespace_template(template: str) -> ParsedTemplate:
    """Validate and tokenize a namespace template."""
    segments: List[_Segment] = []
    last = 0
    for match in _TOKEN_RE.finditer(template):
        start, end = match.span()
        if start > last:
            segments.append(_Literal(template[last:start]))
        token = match.group(1)
        if token in _V03_DEFERRED_TOKENS:
            raise NamespaceTemplateError(
                f"Namespace token '{{{token}}}' is deferred to v0.3. "
                f"Supported v0.2 tokens: {{namespace}}, {{tag:KEY}}."
            )
        if token == "namespace":
            segments.append(_NamespaceToken())
        elif token.startswith("tag:"):
            key = token[len("tag:") :]
            if not key:
                raise NamespaceTemplateError(
                    "Empty tag key in template: '{tag:}' is not valid."
                )
            segments.append(_TagToken(key))
        else:
            raise NamespaceTemplateError(
                f"Unknown namespace token '{{{token}}}'. "
                f"Supported v0.2 tokens: {{namespace}}, {{tag:KEY}}."
            )
        last = end
    if last < len(template):
        segments.append(_Literal(template[last:]))
    return tuple(segments)


def resolve_namespace(
    parsed: ParsedTemplate,
    namespace: str,
    run_tags: Mapping[str, str],
) -> str:
    """Resolve a pre-parsed template against a namespace and run-tag mapping.

    Unresolved tag values → empty string. Adjacent slashes collapse to one;
    leading and trailing slashes strip.
    """
    parts: List[str] = []
    for seg in parsed:
        if isinstance(seg, _Literal):
            parts.append(seg.text)
        elif isinstance(seg, _NamespaceToken):
            parts.append(namespace)
        elif isinstance(seg, _TagToken):
            parts.append(run_tags.get(seg.key, ""))
    return _normalize("".join(parts))


def _normalize(value: str) -> str:
    collapsed = re.sub(r"/+", "/", value)
    return collapsed.strip("/")
