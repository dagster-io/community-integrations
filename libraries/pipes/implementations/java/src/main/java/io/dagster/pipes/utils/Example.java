package io.dagster.pipes.utils;

import java.util.HashMap;
import java.util.Map;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.PipesContext;
import io.dagster.pipes.PipesSession;
import io.dagster.pipes.loaders.PipesContextLoader;
import io.dagster.pipes.loaders.PipesDefaultContextLoader;
import io.dagster.pipes.loaders.PipesEnvVarParamsLoader;
import io.dagster.pipes.loaders.PipesParamsLoader;
import io.dagster.pipes.writers.PipesDefaultMessageWriter;
import io.dagster.pipes.writers.PipesMessageWriter;
import io.dagster.pipes.writers.PipesMessageWriterChannel;

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
        context.reportCustomMessage("Hello from external process!");

        final Map<String, Integer> people = new HashMap<>();
        people.put("Alice", 25);
        people.put("Bob", 17);
        people.put("Charlie", 18);
        people.put("Diana", 30);
        people.put("Edward", 16);

        int adults = (int) people.values().stream()
            .filter(age -> age >= 18)
            .count();
        final Map<String, Integer> metaMap = new HashMap<>();
        metaMap.put("Number of adults", adults);

        context.reportAssetMaterialization(
            metaMap, null, null
        );
    }
}
