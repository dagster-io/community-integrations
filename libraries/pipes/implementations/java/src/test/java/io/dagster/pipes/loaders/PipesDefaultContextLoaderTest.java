package io.dagster.pipes.loaders;

import org.junit.jupiter.api.Test;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.loaders.PipesDefaultContextLoader;

class PipesDefaultContextLoaderTest {

    @Test
    void testLoadContext() throws DagsterPipesException {
        final String realJsonPath = "src/test/resources/realJson.json";
        final PipesDefaultContextLoader loader = new PipesDefaultContextLoader();
        loader.loadFromFile(realJsonPath);
    }
}
