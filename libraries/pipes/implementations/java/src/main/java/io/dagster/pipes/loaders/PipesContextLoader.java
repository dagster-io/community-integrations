package io.dagster.pipes.loaders;

import java.util.Map;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.data.PipesContextData;

public abstract class PipesContextLoader {
    public abstract PipesContextData loadContext(Map<String, Object> params) throws DagsterPipesException;
}
