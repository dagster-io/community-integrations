package io.dagster.pipes.loaders;

import io.dagster.pipes.DagsterPipesException;
import org.junit.jupiter.api.Test;

class PipesDefaultContextLoaderTest {

    @Test
    void testLoadContext() throws DagsterPipesException {
        final String realJsonPath = "src/test/resources/realJson.json";
        final PipesDefaultContextLoader loader = new PipesDefaultContextLoader();
        loader.loadFromFile(realJsonPath);
    }
}
