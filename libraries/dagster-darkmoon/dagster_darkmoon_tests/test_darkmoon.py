from __future__ import annotations

import json
from email.message import Message
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request

import pytest
from dagster import AssetKey, Definitions, asset

from dagster_darkmoon import (
    DarkmoonClient,
    DarkmoonError,
    DarkmoonResource,
    build_darkmoon_findings_check,
)
from dagster_darkmoon.checks import evaluate_findings


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def __enter__(self) -> FakeResponse:  # noqa: PYI034
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


FINDINGS = [
    {
        "id": "1",
        "title": "Stored XSS",
        "severity": "high",
        "status": "exploited",
        "endpoint": "https://app.example.com/comments",
    },
    {
        "id": "2",
        "title": "Missing header",
        "severity": "low",
        "status": "confirmed",
        "endpoint": "https://app.example.com/",
    },
    {
        "id": "3",
        "title": "Possible SQLi",
        "severity": "critical",
        "status": "unconfirmed",
        "endpoint": "https://app.example.com/search",
    },
    {
        "id": "4",
        "title": "Other host",
        "severity": "critical",
        "status": "exploited",
        "endpoint": "https://other.example.org/",
    },
]


def make_opener(routes: dict[tuple[str, str], Any], seen: list[Request]):
    def opener(request: Request, timeout: float) -> FakeResponse:
        seen.append(request)
        key = (request.get_method(), request.full_url.split("/api/v1", 1)[1])
        if key not in routes:
            raise HTTPError(request.full_url, 404, "Not Found", Message(), None)
        return FakeResponse(routes[key])

    return opener


def test_client_requires_credentials() -> None:
    with pytest.raises(ValueError):
        DarkmoonClient("https://dm.test")


def test_client_normalizes_base_url_and_sends_token() -> None:
    seen: list[Request] = []
    client = DarkmoonClient(
        "https://dm.test:8000/api/v1/",
        token="tok",
        opener=make_opener({("GET", "/campaigns"): {"data": [{"id": "c1"}]}}, seen),
    )
    assert client.list_campaigns() == [{"id": "c1"}]
    assert seen[0].full_url == "https://dm.test:8000/api/v1/campaigns"
    assert seen[0].get_header("Authorization") == "Bearer tok"


def test_client_logs_in_once_with_username_and_password() -> None:
    seen: list[Request] = []
    client = DarkmoonClient(
        "https://dm.test",
        username="u",
        password="p",
        opener=make_opener(
            {
                ("POST", "/auth/login"): {"token": "jwt"},
                ("GET", "/campaigns"): {"data": []},
            },
            seen,
        ),
    )
    client.list_campaigns()
    client.list_campaigns()
    logins = [r for r in seen if r.full_url.endswith("/auth/login")]
    assert len(logins) == 1
    assert json.loads(logins[0].data) == {"username": "u", "password": "p"}
    assert seen[-1].get_header("Authorization") == "Bearer jwt"


def test_login_without_token_raises() -> None:
    client = DarkmoonClient(
        "https://dm.test",
        username="u",
        password="p",
        opener=make_opener({("POST", "/auth/login"): {}}, []),
    )
    with pytest.raises(DarkmoonError, match="no token"):
        client.list_campaigns()


def test_findings_filters_and_ids_are_url_safe() -> None:
    seen: list[Request] = []
    client = DarkmoonClient(
        "https://dm.test",
        token="t",
        opener=make_opener(
            {
                ("GET", "/vulnerabilities?campaign_id=c+1&severity=high"): {
                    "data": FINDINGS
                },
                ("GET", "/campaigns/a%2Fb/report"): {"overall_risk": "high"},
            },
            seen,
        ),
    )
    assert len(client.list_findings(campaign_id="c 1", severity="high")) == 4
    assert client.get_campaign_report("a/b") == {"overall_risk": "high"}


def test_launch_campaign_posts_only_set_fields() -> None:
    seen: list[Request] = []
    client = DarkmoonClient(
        "https://dm.test",
        token="t",
        opener=make_opener({("POST", "/run/campaign"): {"run_id": "r1"}}, seen),
    )
    result = client.launch_campaign(
        "https://staging.example.com",
        focus=["sql_injection"],
        safe_harbor="ticket SEC-1",
    )
    assert result == {"run_id": "r1"}
    assert json.loads(seen[0].data) == {
        "target": "https://staging.example.com",
        "focus": ["sql_injection"],
        "safe_harbor": "ticket SEC-1",
    }


def test_http_error_maps_to_darkmoon_error() -> None:
    client = DarkmoonClient("https://dm.test", token="t", opener=make_opener({}, []))
    with pytest.raises(DarkmoonError) as error:
        client.list_campaigns()
    assert error.value.status_code == 404


def test_evaluate_findings_proven_only() -> None:
    passed, blocking, counts = evaluate_findings(FINDINGS[:3], "high", True)
    assert not passed
    assert [f["id"] for f in blocking] == ["1"]
    assert counts["critical"] == 1

    passed, blocking, _ = evaluate_findings(FINDINGS[:3], "high", False)
    assert [f["id"] for f in blocking] == ["1", "3"]

    passed, _, _ = evaluate_findings([FINDINGS[1]], "high", True)
    assert passed


def test_invalid_check_arguments() -> None:
    with pytest.raises(ValueError):
        build_darkmoon_findings_check("svc", "https://a.test", fail_on="severe")
    with pytest.raises(ValueError):
        build_darkmoon_findings_check("svc", "")


class FakeDarkmoonResource(DarkmoonResource):
    def list_findings(self, campaign_id=None, severity=None, status=None):
        return FINDINGS


def test_asset_check_fails_on_proven_high_finding() -> None:
    @asset
    def svc() -> None:
        return None

    check = build_darkmoon_findings_check(svc, "https://app.example.com:8443/")
    resource = FakeDarkmoonResource(base_url="https://dm.test", token="t")
    defs = Definitions(
        assets=[svc], asset_checks=[check], resources={"darkmoon": resource}
    )
    result = defs.resolve_implicit_global_asset_job_def().execute_in_process()
    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 1
    evaluation = evaluations[0]
    assert evaluation.asset_key == AssetKey("svc")
    assert not evaluation.passed
    assert evaluation.metadata["blocking_findings"].value == 1
    assert evaluation.metadata["total_findings"].value == 3


def test_asset_check_passes_when_nothing_blocks() -> None:
    @asset
    def svc() -> None:
        return None

    check = build_darkmoon_findings_check(
        "svc", "app.example.com", fail_on="critical", only_proven=True
    )
    resource = FakeDarkmoonResource(base_url="https://dm.test", token="t")
    defs = Definitions(
        assets=[svc], asset_checks=[check], resources={"darkmoon": resource}
    )
    result = defs.resolve_implicit_global_asset_job_def().execute_in_process()
    assert result.get_asset_check_evaluations()[0].passed
