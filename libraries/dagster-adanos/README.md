# dagster-adanos

A Dagster resource for the [Adanos Market Sentiment API](https://adanos.org/).
It exposes the official Adanos Python SDK inside assets and ops, with read-only
stock and crypto sentiment data from Reddit, X / FinTwit, financial news, and
Polymarket.

## Installation

```bash
uv add dagster-adanos
```

Create an API key at [adanos.org/register](https://adanos.org/register), then
provide it through Dagster's environment-variable configuration.

## Usage

```python
from dagster import AssetExecutionContext, Definitions, EnvVar, asset
from dagster_adanos import AdanosResource


@asset(compute_kind="adanos")
def reddit_stock_sentiment(
    context: AssetExecutionContext,
    adanos: AdanosResource,
) -> list[dict]:
    client = adanos.get_client()
    results = client.reddit.trending(
        from_="2026-07-01",
        to="2026-07-07",
        limit=10,
    )
    context.add_output_metadata({"result_count": len(results)})
    return [item.to_dict() for item in results]


defs = Definitions(
    assets=[reddit_stock_sentiment],
    resources={
        "adanos": AdanosResource(api_key=EnvVar("ADANOS_API_KEY")),
    },
)
```

Use explicit inclusive UTC `from_` and `to` dates for reproducible assets and
backfills. The legacy `days` shorthand is deprecated by the API and is not used
in these examples.

The returned client provides the following namespaces:

- `client.reddit` for Reddit stock sentiment
- `client.x` for X / FinTwit stock sentiment
- `client.news` for financial news sentiment
- `client.polymarket` for stock-related prediction-market signals
- `client.crypto` for Reddit crypto sentiment
- `client.sentiment` for finance-tuned text sentiment analysis

See the [Adanos API documentation](https://api.adanos.org/docs) and
[Python SDK documentation](https://github.com/adanos-software/adanos-python-sdk)
for the complete method and response reference.

## Plans

Historical windows depend on the Adanos account plan: Free supports up to 30
days, Hobby up to 90 days, and Professional up to 365 days. Raw mention and
text-analysis endpoints require Professional. Aggregate sentiment, trending,
detail, comparison, and market-sentiment methods remain available according to
the configured plan and quota.

## Development

```bash
make install
make test
make ruff
make check
make build
```
