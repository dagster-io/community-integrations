# dagster-livetennisapi

A [Dagster](https://dagster.io/) integration for the
[Live Tennis API](https://livetennisapi.com) — real-time tennis scores,
players, rankings, fixtures and match data for ATP, WTA, Challenger and ITF,
wrapped via the official [`livetennisapi`](https://pypi.org/project/livetennisapi/)
Python SDK.

It provides:

- `LiveTennisApiResource` — a `ConfigurableResource` that yields a configured
  `livetennisapi.LiveTennisAPI` client, so assets and ops can call any SDK
  method.
- Asset factories for the **free-tier** surface (keyed; 30 requests/minute,
  100 requests/day):
  - `build_fixtures_asset()` — upcoming scheduled fixtures (a daily fixtures
    sync fits the free tier honestly: a full materialization is typically one
    to a few requests at the maximum page size of 200),
  - `build_players_asset()` — player search, including each player's current
    ranking, with a run-time configurable search string,
  - `build_live_matches_asset()` — a snapshot of matches currently in play.

## Installation

```sh
pip install dagster-livetennisapi
```

## Prerequisites

A Live Tennis API key — the free tier is self-serve with no card at
[livetennisapi.com/subscribe/free](https://livetennisapi.com/subscribe/free).
Put it in the `LIVETENNISAPI_KEY` environment variable.

## Usage

```python
import dagster as dg

from dagster_livetennisapi import (
    LiveTennisApiResource,
    build_fixtures_asset,
    build_live_matches_asset,
    build_players_asset,
)

fixtures = build_fixtures_asset()
players = build_players_asset(default_search="alcaraz")
live_matches = build_live_matches_asset()

# A daily fixtures sync fits comfortably inside the free tier.
fixtures_job = dg.define_asset_job("daily_fixtures_sync", selection=[fixtures])
daily_fixtures_schedule = dg.ScheduleDefinition(
    job=fixtures_job, cron_schedule="0 6 * * *"
)

defs = dg.Definitions(
    assets=[fixtures, players, live_matches],
    schedules=[daily_fixtures_schedule],
    resources={
        "livetennisapi": LiveTennisApiResource(
            api_key=dg.EnvVar("LIVETENNISAPI_KEY"),
        )
    },
)
```

Or use the resource directly in your own assets — `get_client()` yields the
full SDK client:

```python
import dagster as dg
from dagster_livetennisapi import LiveTennisApiResource


@dg.asset
def tournament_catalogue(livetennisapi: LiveTennisApiResource) -> list[dict]:
    with livetennisapi.get_client() as client:
        return [t.to_dict() for t in client.paginate("list_tournaments")]
```

## API tiers

Live Tennis API access is tiered, and a call above your key's tier fails
loudly with `livetennisapi.UpgradeRequired` (HTTP 403) — never with silently
degraded data:

| Tier  | Adds                                                                                   | Rate limits          |
| ----- | -------------------------------------------------------------------------------------- | -------------------- |
| FREE  | Live/upcoming matches, scores, players (incl. current ranking), fixtures, tournaments, usage | 30/min, 100/day      |
| BASIC | Completed-match history, point-by-point tape, results archive (1968–2022), head-to-head | 60/min, 1,000/day    |
| PRO   | Match events, market prices, bulk history packages, rank-ordered rankings listing       | 300/min, 10,000/day  |
| ULTRA | Model analysis / win probability, in-play statistics, per-player as-of rankings, WebSocket | 600/min, 500,000/day |

### History assets (BASIC+ key required)

The built-in factories deliberately stay on the free tier. If your key is
BASIC or higher, a completed-match history asset is a few lines with the same
resource — `client.list_completed_matches(...)` and `client.get_match_tape(...)`
are the relevant SDK methods:

```python
import dagster as dg
from dagster_livetennisapi import LiveTennisApiResource


@dg.asset(description="Yesterday's completed matches (requires a BASIC+ key).")
def completed_matches(livetennisapi: LiveTennisApiResource) -> list[dict]:
    from datetime import date, timedelta

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    with livetennisapi.get_client() as client:
        return [
            match.to_dict()
            for match in client.paginate(
                "list_completed_matches", from_=yesterday, to=yesterday
            )
        ]
```

On a FREE key this raises `livetennisapi.UpgradeRequired` at run time.

## Development

```sh
make install  # uv sync
make test     # uv run pytest (fully mocked; no network)
make ruff     # lint + format
make check    # type check with ty
```
