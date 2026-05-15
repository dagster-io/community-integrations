# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

dagster-evidence is a Dagster integration for [Evidence.dev](https://evidence.dev/), enabling orchestration of Evidence dashboard projects as Dagster assets. It automatically discovers sources from Evidence projects, generates corresponding Dagster assets, and handles build/deployment pipelines.

## Development Commands

```bash
make install   # Install dependencies (uv sync)
make build     # Build package (uv build)
make test      # Run all tests (uv run pytest)
make ruff      # Format and lint (ruff check --fix && ruff format)
make check     # Type checking (uv run pyright)
make quality   # Run all checks (ruff, format, pyright, pytest)

# Run specific tests
uv run pytest dagster_evidence_tests/test_component.py
uv run pytest dagster_evidence_tests/test_translator.py::test_custom_translator
```

**IMPORTANT**: Always run `make quality` after making changes to ensure all checks pass before committing.

## Architecture

### Core Component Flow

```
EvidenceProjectComponentV2 (StateBackedComponent)
    │
    ├── BaseEvidenceProject (LocalEvidenceProject | EvidenceStudioProject)
    │       │
    │       ├── parse_evidence_project() → EvidenceProjectData
    │       │       └── Reads sources/ folder, parses connection.yaml + SQL files
    │       │
    │       └── load_evidence_project_assets() → (AssetsDefinition[], SensorDefinition[])
    │               └── Uses DagsterEvidenceTranslator to convert sources to assets
    │
    └── DagsterEvidenceTranslator
            │
            ├── SOURCE_TYPE_REGISTRY: maps source types to source classes
            │       duckdb → DuckdbEvidenceProjectSource
            │       motherduck → MotherDuckEvidenceProjectSource
            │       bigquery → BigQueryEvidenceProjectSource
            │       gsheets → GSheetsEvidenceProjectSource
            │
            └── get_asset_spec(data) → AssetSpec | AssetsDefinition
                    └── Delegates to source class for source assets
```

### Key Classes

| Class | Location | Purpose |
|-------|----------|---------|
| `EvidenceProjectComponentV2` | `components/evidence_project_v2.py` | Main Dagster component, state-backed |
| `DagsterEvidenceTranslator` | `components/translator.py` | Converts Evidence objects to Dagster assets |
| `BaseEvidenceProjectSource` | `components/sources.py` | Abstract base for data source types |
| `LocalEvidenceProject` | `components/projects.py` | Handles local file-based Evidence projects |
| `BaseEvidenceProjectDeployment` | `components/deployments.py` | Abstract base for deployment targets |

### Source Asset Generation

Each source type (DuckDB, BigQuery, etc.) implements:
1. `extract_data_from_source()` - Parses SQL to extract table dependencies using `sqlglot`
2. `get_source_asset()` - Creates `AssetsDefinition` with automation conditions
3. `get_source_sensor()` - Optional sensor for detecting upstream data changes

### Translator Data Types

- `EvidenceSourceTranslatorData` - Passed to translator for source query assets (contains `source_content`, `source_group`, `query`, `extracted_data`)
- `EvidenceProjectTranslatorData` - Passed to translator for project build asset (contains `project_name`, `sources_by_id`, `source_deps`)

### Deployment Types

- `GithubPagesEvidenceProjectDeployment` - Pushes to GitHub Pages branch using GitPython
- `CustomEvidenceProjectDeployment` - Runs arbitrary shell command
- `EvidenceProjectNetlifyDeployment` - Planned (not yet implemented)

## Extension Points

### Custom Translator
Subclass `DagsterEvidenceTranslator` to customize asset generation:
```python
class MyTranslator(DagsterEvidenceTranslator):
    SOURCE_TYPE_REGISTRY = {
        **DagsterEvidenceTranslator.SOURCE_TYPE_REGISTRY,
        "postgres": PostgresEvidenceProjectSource,
    }

    def get_asset_spec(self, data):
        spec = super().get_asset_spec(data)
        # customize...
        return spec
```

### Adding a New Source Type

To add a new source type (e.g., Postgres, MySQL, Snowflake):

1. **Add the source class** in `components/sources.py`:
   ```python
   @beta
   @public
   class PostgresEvidenceProjectSource(BaseEvidenceProjectSource):
       @staticmethod
       def get_source_type() -> str:
           return "postgres"  # Must match 'type' in connection.yaml

       @classmethod
       def get_hide_source_asset_default(cls) -> bool:
           return True  # Set to True if source assets should be hidden by default

       @classmethod
       def get_source_sensor_enabled_default(cls) -> bool:
           return True  # Set to True to enable change detection sensors

       @classmethod
       def extract_data_from_source(cls, data: "EvidenceSourceTranslatorData") -> dict[str, Any]:
           from dagster_evidence.utils import extract_table_references
           options = data.source_content.connection.extra.get("options", {})
           table_refs = extract_table_references(
               data.query.content,
               default_database=options.get("database"),
               default_schema=options.get("schema", "public"),
           )
           return {"table_deps": table_refs}

       @classmethod
       def get_source_asset(cls, data: "EvidenceSourceTranslatorData") -> dg.AssetsDefinition:
           deps = [dg.AssetKey([ref["table"]]) for ref in data.extracted_data.get("table_deps", []) if ref.get("table")]
           key = dg.AssetKey([data.source_group, data.query.name])
           has_deps = bool(deps)

           @dg.asset(
               key=key,
               group_name=data.source_group,
               kinds={"evidence", "source", "postgres"},
               deps=deps,
               automation_condition=dg.AutomationCondition.any_deps_match(
                   dg.AutomationCondition.newly_updated()
               ) if has_deps else None,
           )
           def _source_asset():
               return dg.MaterializeResult()

           return _source_asset

       @classmethod
       def get_source_sensor(cls, data: "EvidenceSourceTranslatorData", asset_key: dg.AssetKey) -> dg.SensorDefinition | None:
           # Optional: implement change detection sensor
           return None
   ```

2. **Register in translator** - Add to `SOURCE_TYPE_REGISTRY` in `components/translator.py`:
   ```python
   SOURCE_TYPE_REGISTRY: dict[str, type[BaseEvidenceProjectSource]] = {
       "duckdb": DuckdbEvidenceProjectSource,
       # ... existing sources
       "postgres": PostgresEvidenceProjectSource,  # Add new source
   }
   ```

3. **Add dependencies as OPTIONAL** in `pyproject.toml`:
   ```toml
   [project.optional-dependencies]
   postgres = ["psycopg2-binary>=2.9.0"]  # New optional group
   all = [..., "psycopg2-binary>=2.9.0"]  # Add to 'all' group too

   [tool.uv]
   dev-dependencies = [
       # ... existing deps
       "psycopg2-binary>=2.9.0",  # Add for local development
   ]
   ```

4. **Handle import gracefully** - If the source needs the optional library, guard the import:
   ```python
   try:
       import psycopg2
   except ImportError:
       raise ImportError(
           "psycopg2 is required for Postgres sources. "
           "Install it with: pip install dagster-evidence[postgres]"
       ) from None
   ```

5. **Add tests** in `dagster_evidence_tests/test_sources.py`

6. **Update README.md** - Add the new source to the supported data sources list

7. **Run `make quality`** to ensure all checks pass

### Custom Deployment
Subclass `BaseEvidenceProjectDeployment` and implement `deploy_evidence_project()`.

## Optional Dependencies

The package has optional dependency groups:
- `duckdb` - DuckDB/MotherDuck source support and sensors
- `bigquery` - BigQuery source support and sensors
- `gsheets` - Google Sheets source support and sensors
- `github-pages` - GitHub Pages deployment (GitPython)
- `sql` - SQL parsing with sqlglot (for table dependency extraction)

**IMPORTANT**: When adding new sources or features that require external libraries, always add them as optional dependencies. The base package should only depend on `dagster` and `pyyaml`. This keeps the installation lightweight for users who only need specific source types.

## Evidence Project Structure Expected

```
my-evidence-project/
├── package.json
├── evidence.config.yaml
└── sources/
    └── <source_name>/
        ├── connection.yaml    # type, connection options
        └── *.sql              # Query files (except gsheets)
```
