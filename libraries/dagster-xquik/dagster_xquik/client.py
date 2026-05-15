from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

JsonObject = dict[str, Any]


class ResponseLike(Protocol):
    def __enter__(self) -> "ResponseLike": ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    def read(self) -> bytes: ...


RequestOpener = Callable[[Request, float], ResponseLike]

DEFAULT_BASE_URL = "https://xquik.com/api/v1"
DEFAULT_CONTRACT_VERSION = "2026-04-29"


class XquikError(Exception):
    """Raised when Xquik returns an error response or invalid JSON."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class XquikClient:
    """Small read-only client for the Xquik REST API."""

    api_key: str
    base_url: str = DEFAULT_BASE_URL
    contract_version: str = DEFAULT_CONTRACT_VERSION
    timeout_seconds: float = 30.0
    opener: RequestOpener | None = None

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("api_key is required")
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

    def search_tweets(
        self,
        *,
        q: str,
        query_type: str = "Latest",
        cursor: str | None = None,
        since_time: str | None = None,
        until_time: str | None = None,
        limit: int | None = None,
    ) -> JsonObject:
        """Search public tweets by keyword, phrase, hashtag, or account query."""

        return self._get(
            "/x/tweets/search",
            {
                "q": q,
                "queryType": query_type,
                "cursor": cursor,
                "sinceTime": since_time,
                "untilTime": until_time,
                "limit": limit,
            },
        )

    def get_tweet(self, tweet_id: str) -> JsonObject:
        """Fetch one tweet by numeric tweet ID."""

        return self._get(f"/x/tweets/{_path_part(tweet_id, 'tweet_id')}")

    def search_users(self, *, q: str, cursor: str | None = None) -> JsonObject:
        """Search public X/Twitter users by keyword or handle."""

        return self._get("/x/users/search", {"q": q, "cursor": cursor})

    def get_user(self, user_id: str) -> JsonObject:
        """Fetch a public X/Twitter user by numeric ID or handle."""

        return self._get(f"/x/users/{_path_part(user_id, 'user_id')}")

    def get_user_tweets(
        self,
        user_id: str,
        *,
        cursor: str | None = None,
        include_replies: bool | None = None,
        include_parent_tweet: bool | None = None,
        limit: int | None = None,
    ) -> JsonObject:
        """Fetch public tweets from a user timeline."""

        return self._get(
            f"/x/users/{_path_part(user_id, 'user_id')}/tweets",
            {
                "cursor": cursor,
                "includeReplies": include_replies,
                "includeParentTweet": include_parent_tweet,
                "limit": limit,
            },
        )

    def get_trends(self, *, woeid: int = 1, count: int = 20) -> JsonObject:
        """Fetch X/Twitter trends for a WOEID region."""

        return self._get("/x/trends", {"woeid": woeid, "count": count})

    def _get(
        self,
        path: str,
        params: Mapping[str, object | None] | None = None,
    ) -> JsonObject:
        query = _encode_query(params or {})
        url = f"{self.base_url.rstrip('/')}{path}"
        if query:
            url = f"{url}?{query}"

        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "dagster-xquik/0.1.0",
                "x-api-key": self.api_key,
                "xquik-api-contract": self.contract_version,
            },
            method="GET",
        )

        try:
            if self.opener:
                response = self.opener(request, self.timeout_seconds)
            else:
                response = cast(
                    ResponseLike,
                    urlopen(request, timeout=self.timeout_seconds),
                )
            with response as opened:
                raw = opened.read()
        except HTTPError as error:
            detail = _read_error(error)
            message = f"Xquik request failed with HTTP {error.code}"
            if detail:
                message = f"{message}: {detail}"
            raise XquikError(message, status_code=error.code) from error
        except URLError as error:
            raise XquikError(f"Xquik request failed: {error.reason}") from error

        try:
            payload: object = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as error:
            raise XquikError("Xquik returned invalid JSON") from error

        if not isinstance(payload, dict):
            raise XquikError("Xquik returned a non-object JSON response")
        payload_dict = cast(dict[object, object], payload)
        if not all(isinstance(key, str) for key in payload_dict):
            raise XquikError("Xquik returned non-string JSON object keys")
        return cast(JsonObject, payload_dict)


def _path_part(value: str, name: str) -> str:
    if not value:
        raise ValueError(f"{name} is required")
    return quote(value, safe="")


def _encode_query(params: Mapping[str, object | None]) -> str:
    cleaned: dict[str, str] = {}
    for key, value in params.items():
        if value is None or value == "":
            continue
        if isinstance(value, bool):
            cleaned[key] = str(value).lower()
        else:
            cleaned[key] = str(value)
    return urlencode(cleaned)


def _read_error(error: HTTPError) -> str:
    try:
        return error.read().decode("utf-8").strip()
    except Exception:
        return ""
