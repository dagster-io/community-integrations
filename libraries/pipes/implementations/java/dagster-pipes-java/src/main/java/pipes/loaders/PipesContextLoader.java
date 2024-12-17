package pipes.loaders;

import pipes.DagsterPipesException;
import types.PipesContextData;

import java.util.Map;

public abstract class PipesContextLoader {
    public abstract PipesContextData loadContext(Map<String, Object> params) throws DagsterPipesException;
}
