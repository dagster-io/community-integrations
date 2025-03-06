import dagster as dg


from contextlib import contextmanager


@dg.asset(
    # check_specs=[dg.AssetCheckSpec(name="orders_id_has_no_nulls", asset="count_adult_users")]
)
def count_adult_users(
    context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient
):
    @contextmanager
    def capture_params(*args, **kwargs):
        with pipes_subprocess_client.context_injector.__class__.inject_context(
            pipes_subprocess_client.context_injector, *args, **kwargs
        ) as params:
            print(params)
            with open(params["path"], "r") as f:
                print(f.read())
            yield params

    pipes_subprocess_client.context_injector.inject_context = capture_params

    return pipes_subprocess_client.run(
        context=context,
        command=[
            "java",
            "-cp",
            "build/libs/dagster-pipes-java.jar",
            "io.dagster.pipes.utils.Example",
        ],
    ).get_materialize_result()


defs = dg.Definitions(
    assets=[count_adult_users],
    resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
)
