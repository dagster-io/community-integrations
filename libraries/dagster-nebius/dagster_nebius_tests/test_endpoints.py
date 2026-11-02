from types import SimpleNamespace

import httpx
import pytest
from dagster import Failure
from nebius.api.nebius.ai.v1 import Endpoint, EndpointStatus
from nebius.api.nebius.common.v1 import ResourceMetadata

from dagster_nebius import NebiusEndpointResource


def resource(
    monkeypatch, sdk, handler, state=EndpointStatus.State.RUNNING, urls=None, **kwargs
):
    endpoint = Endpoint(
        metadata=ResourceMetadata(id="endpoint-test", parent_id="project-test"),
        status=EndpointStatus(
            state=state, public_endpoints=urls or ["https://example.nebius.cloud"]
        ),
    )
    service = SimpleNamespace(
        get=lambda *a, **k: SimpleNamespace(wait=lambda: endpoint)
    )
    monkeypatch.setattr(
        "dagster_nebius.endpoints.EndpointServiceClient", lambda _: service
    )
    return NebiusEndpointResource(
        sdk_factory=lambda: sdk,
        endpoint_id="endpoint-test",
        project_id="project-test",
        endpoint_token="APP_SECRET",
        health_path="/health",
        poll_interval=0.001,
        http_client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(handler)
        ),
        **kwargs,
    )


def test_ready_and_request(monkeypatch, sdk):
    seen = []

    def handler(request):
        seen.append(request)
        assert request.headers["Authorization"] == "Bearer APP_SECRET"
        return httpx.Response(200, json={"ok": True})

    endpoint = resource(monkeypatch, sdk, handler)
    assert endpoint.wait_until_ready(timeout=1)["id"] == "endpoint-test"
    assert endpoint.request("POST", "/infer", json={"input": "test"}).json() == {
        "ok": True
    }
    assert [str(r.url) for r in seen] == [
        "https://example.nebius.cloud/health",
        "https://example.nebius.cloud/infer",
    ]


@pytest.mark.parametrize("code", [401, 429, 500, 302])
def test_no_post_retry_or_redirect(monkeypatch, sdk, code):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(code, headers={"Location": "https://evil.example/"})

    endpoint = resource(monkeypatch, sdk, handler)
    with pytest.raises(Failure):
        endpoint.request("POST", "/infer")
    assert len(seen) == 1


@pytest.mark.parametrize(
    "path",
    ["https://evil.example", "//evil.example", "/\\evil.example", "/infer#secret"],
)
def test_cross_origin_paths_rejected(monkeypatch, sdk, path):
    endpoint = resource(monkeypatch, sdk, lambda r: httpx.Response(200))
    with pytest.raises(ValueError):
        endpoint.request("POST", path)
    sdk.sync_close.assert_not_called()


def test_multiple_urls_need_selection(monkeypatch, sdk):
    endpoint = resource(
        monkeypatch,
        sdk,
        lambda r: httpx.Response(200),
        urls=["https://one.example", "https://two.example"],
    )
    with pytest.raises(Failure, match="Multiple"):
        endpoint.get_endpoint()


def test_stopped_does_not_probe(monkeypatch, sdk):
    endpoint = resource(
        monkeypatch,
        sdk,
        lambda r: pytest.fail("must not probe"),
        state=EndpointStatus.State.STOPPED,
    )
    with pytest.raises(Failure, match="stopped"):
        endpoint.wait_until_ready(timeout=1)


def test_health_auth_failure_not_retried(monkeypatch, sdk):
    calls = []
    endpoint = resource(
        monkeypatch, sdk, lambda r: calls.append(r) or httpx.Response(401)
    )
    with pytest.raises(Failure, match="authentication"):
        endpoint.wait_until_ready(timeout=1)
    assert len(calls) == 1
