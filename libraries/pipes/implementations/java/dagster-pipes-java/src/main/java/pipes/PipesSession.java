package pipes;

import pipes.loaders.PipesContextLoader;
import pipes.loaders.PipesDefaultContextLoader;
import pipes.loaders.PipesEnvVarParamsLoader;
import pipes.loaders.PipesParamsLoader;
import pipes.writers.PipesDefaultMessageWriter;
import pipes.writers.PipesMessageWriter;
import pipes.writers.PipesMessageWriterChannel;

import java.util.logging.Logger;

import static org.mockito.Mockito.mock;

@SuppressWarnings("PMD.AvoidCatchingGenericException")
public class PipesSession {

    private final PipesContext context;
    private static final Logger LOGGER = Logger.getLogger(PipesSession.class.getName());

    public PipesSession(
        PipesParamsLoader paramsLoader,
        PipesContextLoader contextLoader,
        PipesMessageWriter<? extends PipesMessageWriterChannel> messageWriter
    ) throws DagsterPipesException {
        this.context = buildContext(paramsLoader, contextLoader, messageWriter);
    }

    public void runDagsterPipes(ThrowingConsumer runnable) throws DagsterPipesException {
        try {
            runnable.run(this.context);
        } catch (Exception exception) {
            this.context.reportException(exception);
        } finally {
            this.context.close();
        }
    }

    public PipesContext getContext() {
        return context;
    }

    private PipesContext buildContext(
        PipesParamsLoader paramsLoader,
        PipesContextLoader contextLoader,
        PipesMessageWriter<? extends PipesMessageWriterChannel> messageWriter
    ) throws DagsterPipesException {
        if (PipesContext.isInitialized()) {
            return PipesContext.get();
        }

        final PipesParamsLoader actualParamsLoader = paramsLoader == null
            ? new PipesEnvVarParamsLoader() : paramsLoader;

        PipesContext pipesContext;
        if (actualParamsLoader.isDagsterPipesProcess()) {
            final PipesContextLoader actualContextLoader = contextLoader == null
                ? new PipesDefaultContextLoader() : contextLoader;
            final PipesMessageWriter<? extends PipesMessageWriterChannel> actualMessageWriter
                = messageWriter == null
                ? new PipesDefaultMessageWriter() : messageWriter;

            pipesContext = new PipesContext(
                actualParamsLoader, actualContextLoader, actualMessageWriter
            );
        } else {
            emitOrchestrationInactiveWarning();
            pipesContext = mock(PipesContext.class);
        }
        PipesContext.set(pipesContext);
        return pipesContext;
    }

    private static void emitOrchestrationInactiveWarning() {
        LOGGER.warning(
            "This process was not launched by a Dagster orchestration process. All calls to the " +
            "`dagster-pipes` context or attempts to initialize " +
            "`dagster-pipes` abstractions are no-ops."
        );
    }
}