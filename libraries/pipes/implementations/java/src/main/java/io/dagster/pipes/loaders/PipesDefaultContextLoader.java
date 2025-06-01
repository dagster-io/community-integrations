package io.dagster.pipes.loaders;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.data.PipesContextData;
import io.dagster.pipes.utils.PipesUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class PipesDefaultContextLoader extends PipesContextLoader {
    private final static String FILE_PATH_KEY = "path";
    private final static String DIRECT_KEY = "data";

    @Override
    public PipesContextData loadContext(final Map<String, Object> params) throws DagsterPipesException {
        if (params.containsKey(FILE_PATH_KEY)) {
            final String path = PipesUtils.assertParamType(
                params, FILE_PATH_KEY, String.class, PipesDefaultContextLoader.class
            );
            return loadFromFile(path);
        } else if (params.containsKey(DIRECT_KEY)) {
            final Map<String, Object> data = PipesUtils.assertParamType(
                params, DIRECT_KEY, Map.class, PipesDefaultContextLoader.class
            );
            final ObjectMapper mapper = new ObjectMapper();
            return mapper.convertValue(data, PipesContextData.class);
        } else {
            throw new DagsterPipesException(
                String.format(
                    "Invalid params: expected key %s or %s",
                    FILE_PATH_KEY,
                    DIRECT_KEY
                )
            );
        }
    }

    PipesContextData loadFromFile(final String path) throws DagsterPipesException {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final Map<String, Object> data = mapper.readValue(new File(path), Map.class);
            return mapper.convertValue(data, PipesContextData.class);
        } catch (IOException ioe) {
            throw new DagsterPipesException(
                String.format(
                    "Failed to read context data from file: %s",
                    path
                ),
                ioe
            );
        }
    }
}
