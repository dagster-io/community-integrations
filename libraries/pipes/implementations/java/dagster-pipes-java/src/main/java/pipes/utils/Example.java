package pipes.utils;

import pipes.DagsterPipesException;
import pipes.PipesContext;
import pipes.PipesSession;
import pipes.data.PipesMetadata;
import pipes.loaders.PipesContextLoader;
import pipes.loaders.PipesDefaultContextLoader;
import pipes.loaders.PipesEnvVarParamsLoader;
import pipes.loaders.PipesParamsLoader;
import pipes.writers.PipesDefaultMessageWriter;
import pipes.writers.PipesMessageWriter;
import pipes.writers.PipesMessageWriterChannel;
import types.Type;

import java.util.HashMap;
import java.util.Map;

public class Example {

    public static void main(String[] args) throws DagsterPipesException {
        // Create loaders and writers for PipesSession
        final PipesParamsLoader paramsLoader = new PipesEnvVarParamsLoader();
        final PipesContextLoader contextLoader = new PipesDefaultContextLoader();
        final PipesMessageWriter<? extends PipesMessageWriterChannel> messageWriter
            = new PipesDefaultMessageWriter();

        // Create PipesSession and use runDagsterPipes with custom method reference
        final PipesSession session = new PipesSession(paramsLoader, contextLoader, messageWriter);
        session.runDagsterPipes(Example::userMethodExample);
    }

    private static void userMethodExample(PipesContext context) throws DagsterPipesException {
        context.reportCustomMessage("Hello from Java!");

        final Map<String, Integer> people = new HashMap<>();
        people.put("Alice", 25);
        people.put("Bob", 17);
        people.put("Charlie", 18);
        people.put("Diana", 30);
        people.put("Edward", 16);

        int adults = (int) people.values().stream()
            .filter(age -> age >= 18)
            .count();

        context.reportAssetMaterialization(
            adults, null, null
        );
    }
}
