# Dagster resource for Not Diamond ¬◇

Not Diamond is an AI model router that automatically determines which LLM is best-suited to respond to any query, improving LLM output quality by combining multiple LLMs into a **meta-model** that learns when to call each LLM.

## Installation

```bash
pip install dagster-notdiamond
```

## Configuration

Make sure you have followed the steps in the [Quickstart docs][quickstart], and added a Not Diamond API key to your environment.

## Usage

Define a resource for Not Diamond, then create the asset:

```python
from dagster import (
    Definitions,
    EnvVar,
    ScheduleDefinition,
    AssetSelection,
    AssetExecutionContext,
    MaterializeResult,
    asset,
)
from dagster_notdiamond import NotDiamondResource


@asset
def notdiamond_asset(context: AssetExecutionContext, notdiamond: NotDiamondResource) -> MaterializeResult:
    with notdiamond.get_client(context) as client:
        session_id, best_llm = client.model_select(
            model=["openai/gpt-4o", "openai/gpt-4o-mini"],
            messages=[{"role": "user", "content": "Say this is a test"}]
        )
    return MaterializeResult(metadata={"session_id": session_id, "best_llm": str(best_llm)})

notdiamond_schedule = ScheduleDefinition(
    name="notdiamond_schedule",
    target=AssetSelection.all(),
    cron_schedule="17 * * * *",
)

defs = Definitions(
    assets=[notdiamond_asset],
    resources={
        "notdiamond": NotDiamondResource(api_key=EnvVar("NOTDIAMOND_API_KEY")),
    },
    schedules=[notdiamond_schedule],
)
```

Please refer to `examples/example_notdiamond.py` for an example script which you can test by updating
your pyproject.toml as follows (replacing `$PROJECT_HOME` with the path to this project):

```toml
[tool.dagster]
module_name = "examples.example_notdiamond"
working_directory = "$PROJECT_HOME/libraries/dagster-notdiamond/"
```

## Support

Visit the [documentation for Not Diamond][docs] or send a message to [support@notdiamond.ai][mailto]

[docs]: https://docs.notdiamond.ai
[mailto]: mailto:support@notdiamond.ai
[quickstart]: https://docs.notdiamond.ai/docs/quickstart
