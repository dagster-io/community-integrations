package io.dagster.pipes.loaders;

import io.dagster.pipes.DagsterPipesException;
import java.util.Map;
import java.util.Optional;

public interface PipesParamsLoader {
    boolean isDagsterPipesProcess();

    Optional<Map<String, Object>> loadContextParams() throws DagsterPipesException;

    Optional<Map<String, Object>> loadMessagesParams() throws DagsterPipesException;
}
