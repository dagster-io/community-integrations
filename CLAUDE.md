# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is the Dagster Community Integrations repository - a collection of community-built and maintained integrations for the Dagster data orchestration platform. The repository provides a centralized place for custom integrations without requiring individual maintainers to manage their own build and release processes.

## Repository Structure

- `libraries/` - Contains all integration packages (e.g., dagster-anthropic, dagster-polars, etc.)
- `libraries/_template/` - Template for creating new integrations
- `pipes/implementations/` - Dagster Pipes implementations for Java, Rust, and TypeScript

## Development Commands

All integrations use standardized Makefiles with these commands:

```bash
# Install dependencies
make install  # or: uv sync

# Build package
make build    # or: uv build

# Run tests
make test     # or: uv run pytest

# Format and lint code
make ruff     # Runs: uv run ruff check --fix && uv run ruff format

# Type checking
make check    # Runs: uv run pyright
```

### Running Individual Tests

```bash
# Run a specific test file
uv run pytest dagster_<package>_tests/test_specific.py

# Run a specific test function
uv run pytest dagster_<package>_tests/test_specific.py::test_function_name

# Run tests with verbose output
uv run pytest -v
```

## Creating New Integrations

1. Navigate to `libraries/` directory
2. Copy the template: `cp -r _template dagster-<my-package>`
3. Replace all instances of `example-integration` with your package name
4. Update version in `dagster_<package>/__init__.py`
5. Create GitHub Actions workflows from templates in `.github/workflows/template-*`

## Code Quality Standards

- **Python Version**: Default to Python 3.11
- **Package Management**: Use `uv` exclusively (never pip, poetry, or conda)
- **Formatting**: Enforced with Ruff (`ruff format`)
- **Linting**: Enforced with Ruff (`ruff check --fix`)
- **Type Checking**: Required with Pyright (`pyright`)
- **Testing**: Required with pytest

## Release Process

```bash
# Create a release tag
./release.sh dagster-<package> X.X.X

# Tags follow pattern: <integration-name>-X.X.X
```

## Integration Architecture

Each integration follows this structure:
```
dagster-<package>/
├── Makefile              # Standard development commands
├── README.md             # Documentation with usage examples
├── pyproject.toml        # Package configuration and dependencies
├── uv.lock               # Locked dependencies
├── dagster_<package>/    # Main package code
│   ├── __init__.py       # Contains __version__
│   └── ...               # Integration modules
└── dagster_<package>_tests/
    └── test_*.py         # Test files
```

### Key Architectural Patterns

1. **Dagster Resources**: Integrations typically implement Dagster resources for external services
2. **IO Managers**: For data storage integrations, implement custom IO managers
3. **Ops and Assets**: Provide pre-built ops and assets for common operations
4. **Type Annotations**: Use type hints throughout the codebase
5. **Error Handling**: Wrap external API calls with appropriate error handling

## Testing Local Changes

```bash
# Run GitHub Actions locally with act
act -W .github/workflows/quality-check-dagster-<package>.yml

# For testcontainers support
act --env TESTCONTAINERS_HOST_OVERRIDE=`ipconfig getifaddr en0` -W .github/workflows/quality-check-dagster-<package>.yml
```

## Important Notes

- All tests, formatting, and type checking must pass before merging
- Follow existing patterns in similar integrations
- Include comprehensive documentation with usage examples
- Version is managed in `__init__.py` as `__version__ = "X.X.X"`
- Dependencies are specified in `pyproject.toml` and locked with `uv.lock`