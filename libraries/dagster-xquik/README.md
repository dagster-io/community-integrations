# dagster-xquik

`dagster-xquik` provides a Dagster resource for read-only X/Twitter data workflows through the Xquik REST API.

Use it when Dagster assets or jobs need to:

- Search public tweets
- Fetch a tweet by ID
- Search users
- Fetch a user profile by ID or handle
- Fetch user timeline tweets
- Fetch regional trends

## Installation

`dagster-xquik` is pending its first release through the Dagster community integrations release flow. Until that release is published, install it from a local checkout:

```sh
cd libraries/dagster-xquik
pip install -e .
```

After the first release is published, install the package from PyPI:

```sh
pip install dagster-xquik
```

## Usage

Set `XQUIK_API_KEY` in the environment, then pass it through Dagster's `EnvVar`.

```python
from dagster import AssetExecutionContext, EnvVar, asset, Definitions
from dagster_xquik import XquikResource


@asset
def recent_xquik_tweets(
    context: AssetExecutionContext,
    xquik: XquikResource,
) -> list[dict[str, object]]:
    response = xquik.get_client().search_tweets(q="xquik", limit=10)
    tweets = response.get("data", [])
    context.log.info("Fetched %s tweets", len(tweets))
    return tweets


defs = Definitions(
    assets=[recent_xquik_tweets],
    resources={
        "xquik": XquikResource(api_key=EnvVar("XQUIK_API_KEY")),
    },
)
```

## Resource

```python
from dagster import EnvVar
from dagster_xquik import XquikResource

xquik = XquikResource(api_key=EnvVar("XQUIK_API_KEY"))
client = xquik.get_client()

client.search_tweets(q="dagster", limit=25)
client.get_tweet("1912345678901234567")
client.search_users(q="dagster")
client.get_user("@dagster")
client.get_user_tweets("@dagster", limit=20)
client.get_trends(woeid=1, count=10)
```

`XquikResource` accepts:

| Field | Default | Description |
| --- | --- | --- |
| `api_key` | Required | Xquik API key. Use `EnvVar("XQUIK_API_KEY")` in Dagster definitions. |
| `base_url` | `https://xquik.com/api/v1` | Xquik API base URL. |
| `contract_version` | `2026-04-29` | API contract header version. |
| `timeout_seconds` | `30.0` | HTTP request timeout. |

## Local Development

```sh
make install
make test
make ruff
make check
make build
```
