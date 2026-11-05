from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

JsonObject = dict[str, Any]

SEVERITIES = ("info", "low", "medium", "high", "critical")
PROVEN_STATUSES = ("exploited", "confirmed")

RequestOpener = Callable[[Request, float], Any]


class DarkmoonError(Exception):
    """Raised when Darkmoon cannot be reached or returns an error response."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def normalize_base_url(base_url: str) -> str:
    """Return the API root, accepting the server root or the full ``/api/v1`` URL."""
    base_url = base_url.strip().rstrip("/")
    if not base_url.startswith(("http://", "https://")):
        raise ValueError("base_url must start with http:// or https://")
    base_url = base_url.removesuffix("/api/v1")
    return base_url + "/api/v1"


def _default_opener(request: Request, timeout: float) -> Any:
    return urlopen(request, timeout=timeout)


class DarkmoonClient:
    """Small client for the Darkmoon dashboard API (``/api/v1``).

    The dashboard API is a Darkmoon Pro component. Authenticate with an API
    ``token`` or with a ``username`` and ``password`` (``POST /auth/login``).
    """

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout_seconds: float = 30.0,
        opener: RequestOpener | None = None,
    ) -> None:
        if not token and not (username and password):
            raise ValueError("Provide either token, or both username and password")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.api_base = normalize_base_url(base_url)
        self._token = token or None
        self._username = username
        self._password = password
        self._timeout = timeout_seconds
        self._opener: RequestOpener = opener or _default_opener

    def _send(
        self, method: str, path: str, *, body: Mapping[str, Any] | None, auth: bool
    ) -> Any:
        headers = {"Accept": "application/json"}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if auth:
            headers["Authorization"] = "Bearer " + self._get_token()
        request = Request(
            self.api_base + path, data=data, headers=headers, method=method
        )
        try:
            with self._opener(request, self._timeout) as response:
                payload = response.read()
        except HTTPError as error:
            raise DarkmoonError(
                f"Darkmoon returned HTTP {error.code} for {method} {path}",
                status_code=error.code,
            ) from error
        except URLError as error:
            raise DarkmoonError(f"Unable to reach Darkmoon: {error.reason}") from error
        try:
            return json.loads(payload.decode("utf-8")) if payload else {}
        except ValueError as error:
            raise DarkmoonError(f"Invalid JSON from Darkmoon for {path}") from error

    def _get_token(self) -> str:
        if self._token is None:
            response = self._send(
                "POST",
                "/auth/login",
                body={"username": self._username, "password": self._password},
                auth=False,
            )
            token = response.get("token") if isinstance(response, dict) else None
            if not token:
                raise DarkmoonError("Darkmoon login returned no token")
            self._token = str(token)
        return self._token

    def _request(
        self, method: str, path: str, body: Mapping[str, Any] | None = None
    ) -> Any:
        return self._send(method, path, body=body, auth=True)

    @staticmethod
    def _records(response: Any) -> list[JsonObject]:
        data = response.get("data") if isinstance(response, dict) else response
        return (
            [item for item in data if isinstance(item, dict)]
            if isinstance(data, list)
            else []
        )

    def list_campaigns(self) -> list[JsonObject]:
        return self._records(self._request("GET", "/campaigns"))

    def get_campaign(self, campaign_id: str) -> JsonObject:
        response = self._request(
            "GET", f"/campaigns/{quote(str(campaign_id), safe='')}"
        )
        data = response.get("data", response) if isinstance(response, dict) else {}
        return data if isinstance(data, dict) else {}

    def get_campaign_report(self, campaign_id: str) -> JsonObject:
        response = self._request(
            "GET", f"/campaigns/{quote(str(campaign_id), safe='')}/report"
        )
        return response if isinstance(response, dict) else {}

    def list_findings(
        self,
        campaign_id: str | None = None,
        severity: str | None = None,
        status: str | None = None,
    ) -> list[JsonObject]:
        """List findings (vulnerabilities), optionally filtered server side."""
        params = {
            key: value
            for key, value in (
                ("campaign_id", campaign_id),
                ("severity", severity),
                ("status", status),
            )
            if value
        }
        path = "/vulnerabilities" + ("?" + urlencode(params) if params else "")
        return self._records(self._request("GET", path))

    def launch_campaign(
        self,
        target: str,
        out_of_scope: list[str] | None = None,
        focus: list[str] | None = None,
        noise: str | None = None,
        safe_harbor: str | None = None,
    ) -> JsonObject:
        """Start a campaign (``POST /run/campaign``). Only scan systems you are authorized to test."""
        body: JsonObject = {"target": target}
        for key, value in (
            ("out_of_scope", out_of_scope),
            ("focus", focus),
            ("noise", noise),
            ("safe_harbor", safe_harbor),
        ):
            if value:
                body[key] = value
        response = self._request("POST", "/run/campaign", body)
        return response if isinstance(response, dict) else {}
