package pipes;

import pipes.data.PipesContextData;
import pipes.loaders.PipesDefaultContextLoader;
import pipes.loaders.PipesEnvVarParamsLoader;
import pipes.loaders.PipesMappingParamsLoader;

import java.util.Map;
import java.util.Optional;

public class DataLoader {

    static PipesContextData getData() throws DagsterPipesException {
        PipesMappingParamsLoader paramsLoader = new PipesEnvVarParamsLoader();
        PipesDefaultContextLoader contextLoader = new PipesDefaultContextLoader();
        Optional<Map<String, Object>> params = paramsLoader.loadContextParams();
        if (params.isPresent()) {
            return contextLoader.loadContext(params.get());
        } else {
            throw new DagsterPipesException("Can't load PipesContextData");
        }
    }

    static PipesContextData getData(Map<String, String> input) throws DagsterPipesException {
        if (input.isEmpty()) {
            return getData();
        }
        PipesMappingParamsLoader paramsLoader = new PipesMappingParamsLoader(input);
        PipesDefaultContextLoader contextLoader = new PipesDefaultContextLoader();
        Optional<Map<String, Object>> params = paramsLoader.loadContextParams();
        if (params.isPresent()) {
            return contextLoader.loadContext(params.get());
        } else {
            throw new DagsterPipesException("Can't load PipesContextData");
        }
    }
}