"""Shared test plumbing: a fully mocked Live Tennis API (no network).

The SDK client accepts an httpx ``transport`` keyword, so tests run the real
``livetennisapi`` request/parse path against ``httpx.MockTransport`` — no
sockets are ever opened.
"""

import json
from typing import Any

import httpx
from livetennisapi import LiveTennisAPI

FIXTURES = [
    {
        "id": 9001,
        "event_date": "2026-08-17",
        "start_time": "2026-08-17T15:00:00Z",
        "player1_id": 101,
        "player2_id": 102,
        "player1_name": "Jannik Sinner",
        "player2_name": "Carlos Alcaraz",
        "tour": "atp",
        "tournament": "Cincinnati Masters",
        "round": "F",
        "round_code": "F",
        "surface": "hard",
        "status": "scheduled",
    },
    {
        "id": 9002,
        "event_date": "2026-08-17",
        "start_time": None,
        "player1_id": 201,
        "player2_id": None,
        "player1_name": "Iga Swiatek",
        "player2_name": "Aryna Sabalenka",
        "tour": "wta",
        "tournament": "Cincinnati Masters",
        "round": "F",
        "round_code": "F",
        "surface": "hard",
        "status": "scheduled",
    },
    {
        "id": 9003,
        "event_date": "2026-08-18",
        "start_time": None,
        "player1_id": 301,
        "player2_id": 302,
        "player1_name": "Casper Ruud",
        "player2_name": "Holger Rune",
        "tour": "atp",
        "tournament": "Winston-Salem Open",
        "round": "R32",
        "round_code": "R32",
        "surface": "hard",
        "status": "scheduled",
    },
]

PLAYERS = [
    {
        "id": 101,
        "name": "Jannik Sinner",
        "tour": "atp",
        "country": "ita",
        "ranking": 1,
        "ranking_points": 11480,
        "hand": "right",
    },
]

LIVE_MATCHES = [
    {
        "id": 555,
        "tournament": "Cincinnati Masters",
        "tournament_id": "cincinnati",
        "tour": "atp",
        "surface": "hard",
        "round": "SF",
        "round_code": "SF",
        "status": "live",
        "is_doubles": False,
        "players": {
            "player1": {"id": 101, "name": "Jannik Sinner"},
            "player2": {"id": 301, "name": "Casper Ruud"},
        },
        "score": {"sets": [[6, 4], [3, 2]], "serving": 1},
    },
]


class MockLiveTennisApi:
    """Routes SDK requests to canned page responses and records every request."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    def _handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        params = request.url.params
        limit = int(params.get("limit", "50"))
        offset = int(params.get("offset", "0"))
        path = request.url.path

        if path.endswith("/fixtures"):
            rows: list[dict[str, Any]] = FIXTURES
        elif path.endswith("/players"):
            rows = PLAYERS
        elif path.endswith("/matches"):
            rows = LIVE_MATCHES if params.get("status") == "live" else []
        else:
            return httpx.Response(404, json={"error": "not_found"})

        page = rows[offset : offset + limit]
        return httpx.Response(
            200,
            content=json.dumps({"data": page, "meta": {"count": len(page)}}),
            headers={"Content-Type": "application/json"},
        )

    def client_factory(self, **kwargs: Any) -> LiveTennisAPI:
        """Drop-in replacement for the ``LiveTennisAPI`` constructor."""
        return LiveTennisAPI(transport=httpx.MockTransport(self._handler), **kwargs)
