from __future__ import annotations

import json
from email.message import Message
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request

import pytest

from dagster_xquik import XquikClient, XquikError, XquikResource


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def test_search_tweets_builds_expected_request() -> None:
    requests: list[Request] = []

    def opener(request: Request, timeout: float) -> FakeResponse:
        requests.append(request)
        assert timeout == 12.5
        return FakeResponse({"data": [{"id": "1"}]})

    client = XquikClient(
        api_key="test-key",
        base_url="https://example.test/api/v1/",
        timeout_seconds=12.5,
        opener=opener,
    )

    response = client.search_tweets(
        q="dagster xquik",
        query_type="Top",
        cursor="next",
        since_time="2026-05-01T00:00:00Z",
        until_time="2026-05-02T00:00:00Z",
        limit=10,
    )

    assert response == {"data": [{"id": "1"}]}
    assert len(requests) == 1
    request = requests[0]
    assert request.full_url == (
        "https://example.test/api/v1/x/tweets/search"
        "?q=dagster+xquik&queryType=Top&cursor=next"
        "&sinceTime=2026-05-01T00%3A00%3A00Z"
        "&untilTime=2026-05-02T00%3A00%3A00Z&limit=10"
    )
    assert request.get_header("X-api-key") == "test-key"
    assert request.get_header("Xquik-api-contract") == "2026-04-29"


def test_get_user_tweets_encodes_path_and_boolean_params() -> None:
    requests: list[Request] = []

    def opener(request: Request, timeout: float) -> FakeResponse:
        requests.append(request)
        assert timeout == 30.0
        return FakeResponse({"data": []})

    client = XquikClient(api_key="test-key", opener=opener)

    response = client.get_user_tweets(
        "@dagster",
        cursor="page 2",
        include_replies=True,
        include_parent_tweet=False,
    )

    assert response == {"data": []}
    assert len(requests) == 1
    assert requests[0].full_url == (
        "https://xquik.com/api/v1/x/users/%40dagster/tweets"
        "?cursor=page+2&includeReplies=true&includeParentTweet=false"
    )


def test_single_resource_methods_use_source_truth_paths() -> None:
    seen_urls: list[str] = []

    def opener(request: Request, timeout: float) -> FakeResponse:
        seen_urls.append(request.full_url)
        assert timeout == 30.0
        return FakeResponse({"ok": True})

    client = XquikClient(api_key="test-key", opener=opener)

    assert client.get_tweet("1912345678901234567") == {"ok": True}
    assert client.search_users(q="dagster") == {"ok": True}
    assert client.get_user("123456") == {"ok": True}
    assert client.get_trends(woeid=1, count=5) == {"ok": True}
    assert seen_urls == [
        "https://xquik.com/api/v1/x/tweets/1912345678901234567",
        "https://xquik.com/api/v1/x/users/search?q=dagster",
        "https://xquik.com/api/v1/x/users/123456",
        "https://xquik.com/api/v1/x/trends?woeid=1&count=5",
    ]


def test_resource_returns_configured_client() -> None:
    resource = XquikResource(
        api_key="test-key",
        base_url="https://example.test/api",
        contract_version="2026-04-29",
        timeout_seconds=3.0,
    )

    client = resource.get_client()

    assert client.api_key == "test-key"
    assert client.base_url == "https://example.test/api"
    assert client.contract_version == "2026-04-29"
    assert client.timeout_seconds == 3.0


def test_client_validates_required_configuration() -> None:
    with pytest.raises(ValueError, match="api_key is required"):
        XquikClient(api_key="")

    with pytest.raises(ValueError, match="base_url must start"):
        XquikClient(api_key="test-key", base_url="ftp://example.test")

    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        XquikClient(api_key="test-key", timeout_seconds=0)


def test_http_error_includes_status_code() -> None:
    def opener(request: Request, timeout: float) -> FakeResponse:
        headers: Message[str, str] = Message()
        raise HTTPError(
            request.full_url,
            401,
            "Unauthorized",
            hdrs=headers,
            fp=None,
        )

    client = XquikClient(api_key="test-key", opener=opener)

    with pytest.raises(XquikError) as exc_info:
        client.get_tweet("1")

    assert exc_info.value.status_code == 401
    assert "HTTP 401" in str(exc_info.value)


def test_non_object_json_response_is_rejected() -> None:
    def opener(request: Request, timeout: float) -> FakeResponse:
        return FakeResponse([{"id": "1"}])

    client = XquikClient(api_key="test-key", opener=opener)

    with pytest.raises(XquikError, match="non-object JSON"):
        client.search_users(q="dagster")
