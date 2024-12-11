import dagster as dg


@dg.asset(
    #check_specs=[dg.AssetCheckSpec(name="orders_id_has_no_nulls", asset="count_adult_users")]
)
def count_adult_users(
    context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient
):
    context.get_asset_provenance
    return pipes_subprocess_client.run(
        context=context,
        command=["java", "-cp", "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar", "pipes.utils.Example"],
    ).get_materialize_result()


defs = dg.Definitions(
    assets=[count_adult_users],
    resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
)
