from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    AssetsDefinition,
    MetadataValue,
    asset_check,
)

from dagster_darkmoon.client import PROVEN_STATUSES, SEVERITIES
from dagster_darkmoon.resources import DarkmoonResource


def _severity(finding: dict[str, Any]) -> str:
    value = str(finding.get("severity") or "").strip().lower()
    return value if value in SEVERITIES else "info"


def _host(value: Any) -> str | None:
    value = str(value or "").strip()
    if not value:
        return None
    parsed = urlparse(value if "://" in value else "//" + value)
    return parsed.hostname.lower().rstrip(".") if parsed.hostname else None


def evaluate_findings(
    findings: list[dict[str, Any]],
    fail_on: str,
    only_proven: bool,
) -> tuple[bool, list[dict[str, Any]], Counter]:
    """Return ``(passed, blocking_findings, counts_by_severity)``."""
    floor = SEVERITIES.index(fail_on)
    counts: Counter = Counter(_severity(f) for f in findings)
    blocking = [
        f
        for f in findings
        if SEVERITIES.index(_severity(f)) >= floor
        and (
            not only_proven
            or str(f.get("status") or "").strip().lower() in PROVEN_STATUSES
        )
    ]
    return not blocking, blocking, counts


def build_darkmoon_findings_check(
    asset: str | Sequence[str] | AssetKey | AssetsDefinition,
    target: str,
    *,
    fail_on: str = "high",
    only_proven: bool = True,
    resource_key: str = "darkmoon",
    name: str = "darkmoon_findings",
    blocking: bool = False,
):
    """Build an asset check that fails when Darkmoon holds findings for ``target``.

    The check reads findings already produced by Darkmoon campaigns whose
    ``endpoint`` host matches the host of ``target``. It never starts a scan.

    Args:
        asset: The asset (or asset key) the check is attached to, for example a deployed service.
        target: URL or host of the system the asset represents.
        fail_on: Lowest severity that fails the check (``info`` to ``critical``).
        only_proven: Only count findings with status ``exploited`` or ``confirmed``.
        resource_key: Key of the :py:class:`DarkmoonResource` in your definitions.
        name: Name of the asset check.
        blocking: Block downstream assets when the check fails.
    """
    if fail_on not in SEVERITIES:
        raise ValueError(f"fail_on must be one of {', '.join(SEVERITIES)}")
    asset_key = (
        asset.key
        if isinstance(asset, AssetsDefinition)
        else AssetKey.from_coercible(asset)
    )
    target_host = _host(target)
    if target_host is None:
        raise ValueError("target must be a URL or a host name")

    @asset_check(
        asset=asset_key,
        name=name,
        required_resource_keys={resource_key},
        blocking=blocking,
        description=(
            f"Fails when Darkmoon holds {'proven ' if only_proven else ''}findings "
            f"of severity {fail_on} or above for {target_host}."
        ),
    )
    def _check(context) -> AssetCheckResult:
        darkmoon: DarkmoonResource = getattr(context.resources, resource_key)
        findings = [
            f
            for f in darkmoon.list_findings()
            if _host(f.get("endpoint")) == target_host
        ]
        passed, blocking_findings, counts = evaluate_findings(
            findings, fail_on, only_proven
        )
        blocking_findings.sort(key=lambda f: -SEVERITIES.index(_severity(f)))
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.ERROR,
            description=(
                f"{len(blocking_findings)} blocking Darkmoon finding(s) for {target_host}"
                if not passed
                else f"No blocking Darkmoon findings for {target_host}"
            ),
            metadata={
                "target": target_host,
                "total_findings": len(findings),
                "blocking_findings": len(blocking_findings),
                **{
                    severity: counts.get(severity, 0)
                    for severity in reversed(SEVERITIES)
                },
                "top_findings": MetadataValue.json(
                    [
                        {
                            "title": f.get("title"),
                            "severity": _severity(f),
                            "status": f.get("status"),
                            "endpoint": f.get("endpoint"),
                            "cve": f.get("cve"),
                        }
                        for f in blocking_findings[:10]
                    ]
                ),
            },
        )

    return _check
