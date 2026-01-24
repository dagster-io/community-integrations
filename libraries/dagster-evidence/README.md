# dagster-evidence

Dagster integration with [Evidence](https://evidence.dev/), enabling you to orchestrate Evidence.dev dashboard projects as Dagster assets.

![architecture](./docs/component-architecture.excalidraw.png)
## Installation

```bash
# Basic installation
uv add dagster-evidence

# With specific data source support
uv add "dagster-evidence[duckdb]"           # DuckDB support
uv add "dagster-evidence[bigquery]"         # BigQuery support
uv add "dagster-evidence[gsheets]"          # Google Sheets support
uv add "dagster-evidence[github-pages]"     # GitHub Pages deployment

# Install multiple extras
uv add "dagster-evidence[duckdb,bigquery,github-pages]"
```

## Features

- **Automatic asset discovery**: Automatically generates Dagster assets from Evidence project sources
- **Build and deployment**: Builds Evidence projects and deploys them to your hosting platform
- **Customizable translation**: Extend the translator to customize how Evidence sources map to Dagster assets
- **Type-safe configuration**: Pydantic-based configuration with YAML support
- **Source change detection**: Optional sensors to detect changes in upstream data sources

## Supported Data Sources

- DuckDB
- MotherDuck
- BigQuery
- Google Sheets

Need additional source types? You can extend the translator to add custom sources, or contribute to the project!

## Deployment Support

**Currently Implemented:**
- GitHub Pages: Deploy to GitHub Pages with automatic git push
- Custom commands: Run any custom deployment script or command

**Need another deployment target?** Open an issue and tag [@milicevica23](https://github.com/milicevica23) with your deployment requirements.

## Quick Start

### Component Configuration (YAML)

```yaml
# defs.yaml
type: dagster_evidence.EvidenceProjectComponentV2
attributes:
  evidence_project:
    project_type: local
    project_path: ./my-evidence-project
    enable_source_assets_hiding: true
    enable_source_sensors: false
    project_deployment:
      type: github_pages
      github_repo: my-org/my-dashboard
      branch: gh-pages
```

## Advanced Usage

### Custom Translator

You can customize how Evidence sources are translated to Dagster assets:

```python
from dagster_evidence import DagsterEvidenceTranslator, EvidenceSourceTranslatorData
import dagster as dg

class MyTranslator(DagsterEvidenceTranslator):
    def get_asset_spec(self, data):
        if isinstance(data, EvidenceSourceTranslatorData):
            # Custom logic for source assets
            return dg.AssetSpec(
                key=dg.AssetKey(["custom", data.query.name]),
                kinds={"evidence", data.source_content.connection.type},
            )
        return super().get_asset_spec(data)
```


## Contributing

Contributions are welcome! Please see the main repository's contribution guidelines.
