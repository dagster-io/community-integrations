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

## Configuration Options

### Full List

See the [Apprise documentation](https://github.com/caronc/apprise) for all supported services.

### AppriseNotificationsConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `urls` | `list[str]` | `[]` | List of Apprise notification URLs |
| `config_file` | `str` | `None` | Path to Apprise config file |
| `base_url` | `str` | `None` | Base URL for Dagster UI links |
| `title_prefix` | `str` | `"Dagster"` | Prefix for notification titles |
| `events` | `list[str]` | `["FAILURE"]` | Events to notify about |
| `include_jobs` | `list[str]` | `["*"]` | Job patterns to include |
| `exclude_jobs` | `list[str]` | `[]` | Job patterns to exclude |

### Supported Events

- `SUCCESS`: Job completed successfully
- `FAILURE`: Job failed
- `CANCELED`: Job was canceled
- `RUNNING`: Job started running

### Job Filtering

Use wildcard patterns to control which jobs trigger notifications:

```python
config = AppriseNotificationsConfig(
    urls=["mailto://user:pass@gmail.com"],
    include_jobs=["prod_*", "critical_*"],  # Only production and critical jobs
    exclude_jobs=["*_test", "*_dev"],       # Exclude test and dev jobs
    events=["SUCCESS", "FAILURE"],
)
```

## Advanced Usage

### Op-level Notifications with Hooks

```python
from dagster_apprise import apprise_on_failure, apprise_on_success
import dagster as dg

@apprise_on_failure(urls=["mailto://user:pass@gmail.com"])
@dg.job
def critical_job():
    my_op()

# Or apply to specific ops
@dg.job
def selective_notifications():
    my_op.with_hooks({apprise_on_failure(urls=["mailto://user:pass@gmail.com"])})()
```

### Using AppriseResource in Ops

```python
import dagster as dg
from dagster_apprise import AppriseResource

@dg.op
def notify_op(apprise: AppriseResource):
    apprise.notify(title="Custom Alert", body="processing complete")
```

### Using with Environment Variables

```python
import os
from dagster_apprise import AppriseNotificationsConfig

# Load from environment
config = AppriseNotificationsConfig(
    urls=[os.getenv("GMAIL_URL")],
    base_url=os.getenv("DAGSTER_BASE_URL", "http://localhost:3000"),
    events=["SUCCESS", "FAILURE"],
)
```

### YAML Configuration

Create `defs.yaml`:

```yaml
type: dagster_apprise.apprise_notifications
attributes:
  urls:
    - "pover://user@token"
    - "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
  base_url: "http://localhost:3000"
  events: ["SUCCESS", "FAILURE"]
  include_jobs: ["*"]
  exclude_jobs: ["*_test"]
```

### Multiple Notification Channels

```python
config = AppriseNotificationsConfig(
    urls=[
        "pover://user@token",  # Pushover for immediate testing alerts
        "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",  # Slack for team
        "mailto://user:pass@gmail.com",  # Email for backup
    ],
    events=["SUCCESS", "FAILURE"],
)
```

### Different Notifications for Different Jobs

```python
# Critical jobs - notify on all events
critical_config = AppriseNotificationsConfig(
    urls=["mailto://user:pass@gmail.com"],
    include_jobs=["critical_*"],
    events=["SUCCESS", "FAILURE", "CANCELED", "RUNNING"],
)

# Regular jobs - only notify on failures
regular_config = AppriseNotificationsConfig(
    urls=["https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"],
    include_jobs=["*"],
    exclude_jobs=["critical_*"],
    events=["FAILURE"],
)

# Combine both
defs = dg.Definitions(
    assets=[your_assets],
    jobs=[your_jobs],
    resources={
        **apprise_notifications(critical_config).resources,
        **apprise_notifications(regular_config).resources,
    },
    sensors=[
        *apprise_notifications(critical_config).sensors,
        *apprise_notifications(regular_config).sensors,
    ],
)
```

### Custom Run Failure Sensor

```python
from dagster_apprise import make_apprise_on_run_failure_sensor

apprise_on_failure = make_apprise_on_run_failure_sensor(
    urls=["mailto://user:pass@gmail.com"],
    monitored_jobs=["prod_*"]
)

defs = dg.Definitions(sensors=[apprise_on_failure])
```

### Integration with Dagster Logs

```python
@dg.op
def my_op(context):
    context.log.info("This appears in Dagster logs")
    error_condition = False
    if error_condition:
        context.resources.apprise.notify(
            title="Critical Error",
            body=f"Job {context.job_name} encountered an error",
        )
```

### Dagster+ Users

If you're using Dagster+, you already have built-in alerting for Slack, Microsoft Teams, PagerDuty, and email. For OSS users, try `dagster-apprise` for an alternative.

### Notes on Base URL

- You can set `base_url` or `webserver_base_url` (alias) to include deep links to runs in notifications.

## Development

The Makefile provides the tools required to test and lint your local installation.

```bash
make test
make ruff
make check
```
