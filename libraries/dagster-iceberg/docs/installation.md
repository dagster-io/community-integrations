# Installing dagster-iceberg

This library has beta status. For a specific release, you can install it using:

```shell
pip install https://github.com/JasperHG90/dagster-iceberg/releases/download/v<VERSION>/dagster_iceberg-<VERSION>-py3-none-any.whl
```

Or e.g.:

```shell
uv add https://github.com/JasperHG90/dagster-iceberg/releases/download/v<VERSION>/dagster_iceberg-<VERSION>-py3-none-any.whl
```

You can find a list of versions / releases [here](https://github.com/JasperHG90/dagster-iceberg/releases).

The following extras are available:

- daft (for interoperability with Daft dataframes)
- polars (for interoperability with Polars dataframes)
- pandas (for interoperability with Pandas dataframes)

Pyarrow is installed as a default dependency, so that IO Manager is always available.
