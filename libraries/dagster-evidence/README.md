# dagster-evidence

Dagster integration with [Evidence](https://evidence.dev/), enabling you to orchestrate Evidence.dev dashboard projects as Dagster assets.

## Features

- **Automatic asset discovery**: Automatically generates Dagster assets from Evidence project sources
- **Build and deployment**: Builds Evidence projects and deploys them to your hosting platform
- **Customizable translation**: Extend the translator to customize how Evidence sources map to Dagster assets
- **Type-safe configuration**: Pydantic-based configuration with YAML support

## Deployment Support

**Currently Implemented:**
- ✅ **GitHub Pages**: Deploy to GitHub Pages with automatic git push
- ✅ **Custom commands**: Run any custom deployment script or command

**Need another deployment target?** Open an issue and tag [@milicevica23](https://github.com/milicevica23) with your deployment requirements.

## Supported Data Sources

- DuckDB
- MotherDuck
- BigQuery

Need additional source types? You can extend the translator to add custom sources, or contribute to the project!

### TODO
- check if secrets are visible somewhere in the UI when defining in the component

#### Future work:
- add more metadata to the objects
- try out to put metadata in a source/project and read it in the translator for example dagster group where this project should be
- integrate sensors in motherduck and gsheets source to detect changes in the datasets
- add motherduck kind to dagster


 