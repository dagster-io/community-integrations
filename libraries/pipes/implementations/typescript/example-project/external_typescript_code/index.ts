import * as dagster_pipes from 'dagster_pipes_typescript';

using context = dagster_pipes.openDagsterPipes()

context.logger.info("this is a log message from the external process")
context.logger.warning("This is an example warning from the external process")

context.reportAssetMaterialization(
    {
        "row_count": 100
    }
)
context.reportAssetCheck(
    "example_typescript_check", true
)
