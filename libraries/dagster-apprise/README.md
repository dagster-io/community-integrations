# dagster-apprise

A dagster module that provides integration with Apprise for sending notifications across 70+ services including Slack, Discord, Pushover, Telegram, email, and more.

## Installation

The dagster_apprise module is a PyPI package - use whichever python environment manager you'd prefer, although we default to using uv.

```bash
uv venv
source .venv/bin/activate
uv pip install dagster-apprise
```

## Example Usage

```python
import dagster as dg
from dagster_apprise import apprise_notifications, AppriseNotificationsConfig

# Configure notifications
config = AppriseNotificationsConfig(
    urls=["discord://webhook_id/webhook_token"], # Discord notification, details should be secure
    events=["SUCCESS", "FAILURE"],  # What to notify about
    include_jobs=["*"],  # All jobs
)

# Add to your Dagster definitions
defs = dg.Definitions(
    assets=[your_assets],
    jobs=[your_jobs],
    **apprise_notifications(config).to_dict()
)
```

## Development

The Makefile provides the tools required to test and lint your local installation.

```bash
make test
make ruff
make check
```
