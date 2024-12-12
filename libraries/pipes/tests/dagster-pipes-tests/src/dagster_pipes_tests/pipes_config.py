from serde import serde


@serde
class PipesConfig:
    """
    This configuration class describes the capabilities of a specific Pipes implementation.
    """

    s3: bool = False
    databricks: bool = False
