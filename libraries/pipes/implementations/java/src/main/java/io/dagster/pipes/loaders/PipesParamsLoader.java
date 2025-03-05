package io.dagster.pipes.loaders;

import java.util.Map;
import java.util.Optional;

import io.dagster.pipes.DagsterPipesException;

public interface PipesParamsLoader {
    boolean isDagsterPipesProcess();
    Optional<Map<String, Object>> loadContextParams() throws DagsterPipesException;
    Optional<Map<String, Object>> loadMessagesParams() throws DagsterPipesException;
}
