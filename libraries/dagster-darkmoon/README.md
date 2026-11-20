# dagster-darkmoon

Dagster resource and asset checks for [Darkmoon](https://github.com/ASCIT31/Dark-Moon), an open source (GPL-3.0) autonomous AI penetration testing platform. An LLM orchestrates specialist agents and offensive tools, and each finding is backed by evidence from a real exploit attempt.

With this package a Dagster pipeline can:

- read Darkmoon campaigns, findings and reports,
- fail an asset (for example a deployed service) when Darkmoon holds findings above a severity threshold for it, and optionally block downstream assets,
- launch a Darkmoon campaign against a target you are authorized to test.

## Requirements

The Darkmoon **dashboard API** (`/api/v1`, port 8000 by default) is a **Darkmoon Pro** component. The open source edition is a CLI with local JSON and SARIF output and has no REST API for this package to call. Authenticate with an API token, or with a username and password (`POST /api/v1/auth/login`).

## Installation

```sh
pip install dagster-darkmoon
```

## Usage

### Resource

```python
from dagster import Definitions, EnvVar
from dagster_darkmoon import DarkmoonResource

defs = Definitions(
    resources={
        "darkmoon": DarkmoonResource(
            base_url="https://darkmoon.example.com:8000",
            token=EnvVar("DARKMOON_TOKEN"),
        )
    }
)
```

`base_url` accepts the server root or the full `/api/v1` URL. Instead of `token` you can pass `username` and `password`.

```python
from dagster import asset
from dagster_darkmoon import DarkmoonResource


@asset
def critical_findings(darkmoon: DarkmoonResource) -> list[dict]:
    return darkmoon.list_findings(severity="critical", status="exploited")
```

Available methods: `list_campaigns()`, `get_campaign(campaign_id)`, `get_campaign_report(campaign_id)`, `list_findings(campaign_id=None, severity=None, status=None)` and `launch_campaign(target, out_of_scope=None, focus=None, noise=None, safe_harbor=None)`.

`launch_campaign` starts an autonomous pentest. Only run it against systems you are authorized to test.

### Asset check

```python
from dagster import Definitions, EnvVar, asset
from dagster_darkmoon import DarkmoonResource, build_darkmoon_findings_check


@asset
def web_app() -> None: ...  # deploy the service


darkmoon_check = build_darkmoon_findings_check(
    web_app,
    target="https://app.example.com",
    fail_on="high",
    only_proven=True,
    blocking=True,
)

defs = Definitions(
    assets=[web_app],
    asset_checks=[darkmoon_check],
    resources={
        "darkmoon": DarkmoonResource(
            base_url="https://darkmoon.example.com:8000",
            token=EnvVar("DARKMOON_TOKEN"),
        )
    },
)
```

The check reads the findings Darkmoon already holds whose `endpoint` host matches the host of `target`; it never starts a scan. It fails when a finding has severity `fail_on` or above (`info`, `low`, `medium`, `high`, `critical`). With `only_proven=True` (default) only findings with status `exploited` or `confirmed` count; `unconfirmed` findings were not proven by the agents. Metadata reports totals per severity and the top blocking findings.

## Limitations

- No finding for a host only means Darkmoon has not tested it or found nothing. It is not a clean verdict.
- Findings can include false positives and must be reviewed by a qualified human.

## Test

```sh
make test
```

## Build

```sh
make build
```
