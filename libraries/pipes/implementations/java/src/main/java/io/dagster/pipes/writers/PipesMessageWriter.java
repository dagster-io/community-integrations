package io.dagster.pipes.writers;

import java.util.HashMap;
import java.util.Map;

import io.dagster.pipes.DagsterPipesException;

public abstract class PipesMessageWriter<T extends PipesMessageWriterChannel> {

    public abstract T open(Map<String, Object> params) throws DagsterPipesException;

    public final Map<String, Object> getOpenedPayload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("extras", getOpenedExtras());
        return payload;
    }

    public Map<String, Object> getOpenedExtras() {
        return new HashMap<>();
    }
}
