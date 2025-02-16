alias s := setup
alias t := test
alias c := typecheck
alias p := pre_commit
alias d := docs

# Install python dependencies
install:
  uv sync --all-extras --group dev --group docs

# Install pre-commit hooks
pre_commit_setup:
  uv run pre-commit install

# Install python dependencies and pre-commit hooks
setup: install pre_commit_setup

# Run pre-commit
pre_commit:
 uv run pre-commit run -a

# Run pytest
test:
  uv run pytest tests

# Run pyright
typecheck:
  uv run pyright

# Serve docs locally
docs:
  uv run mkdocs serve
