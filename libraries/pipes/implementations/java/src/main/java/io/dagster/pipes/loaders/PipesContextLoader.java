package io.dagster.pipes.loaders;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.data.PipesContextData;
import java.util.Map;

public abstract class PipesContextLoader {
    public abstract PipesContextData loadContext(Map<String, Object> params) throws DagsterPipesException;
}
