package io.dagster.pipes.loaders;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.data.PipesConstants;
import io.dagster.pipes.utils.PipesUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.zip.InflaterInputStream;

public class PipesMappingParamsLoader implements PipesParamsLoader {
    private final Map<String, String> mapping;

    public PipesMappingParamsLoader(Map<String, String> mapping) {
        this.mapping = mapping;
    }

    @Override
    public boolean isDagsterPipesProcess() {
        return this.mapping.containsKey(PipesConstants.CONTEXT_ENV_VAR.name);
    }

    @Override
    public Optional<Map<String, Object>> loadContextParams() throws DagsterPipesException {
        String rawValue = this.mapping.get(PipesConstants.CONTEXT_ENV_VAR.name);
        if (rawValue == null) {
            System.out.printf(
                    "Provided mapping doesn't contain %s%n",
                    PipesConstants.CONTEXT_ENV_VAR.name
            );
            return Optional.empty();
        }
        return Optional.of(PipesUtils.decodeParam(rawValue));
    }

    @Override
    public Optional<Map<String, Object>> loadMessagesParams() throws DagsterPipesException {
        String rawValue = this.mapping.get(PipesConstants.MESSAGES_ENV_VAR.name);
        if (rawValue == null) {
            System.out.printf(
                    "Provided mapping doesn't contain %s%n",
                    PipesConstants.MESSAGES_ENV_VAR.name
            );
            return Optional.empty();
        }
        return Optional.of(PipesUtils.decodeParam(rawValue));
    }
 
}
