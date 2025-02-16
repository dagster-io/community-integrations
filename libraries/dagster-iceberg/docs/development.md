# Development

To start developing, first clone the repository. In VSCode, open the repository in a devcontainer, and execute `just s` to install the dev dependencies and the project.

## Running tests

To run pytest tests, execute `just t`.

## Pre-commit hooks

Pre-commit hooks are installed when you execute `just s`.

## Additional type checking

We use `pyright` for additional type checking checks since that is what the main Dagster repository uses.

To run type checks with pyright, execute `just c`.
