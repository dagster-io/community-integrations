package pipes.loaders;

import org.junit.jupiter.api.Test;
import pipes.DagsterPipesException;

class PipesDefaultContextLoaderTest {

    @Test
    void testLoadContext() throws DagsterPipesException {
        final String realJsonPath = "src/test/resources/realJson.json";
        final PipesDefaultContextLoader loader = new PipesDefaultContextLoader();
        loader.loadFromFile(realJsonPath);
    }
}
