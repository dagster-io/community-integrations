package io.dagster.pipes;

import io.dagster.pipes.loaders.PipesContextLoader;
import io.dagster.pipes.loaders.PipesDefaultContextLoader;
import io.dagster.pipes.loaders.PipesEnvVarParamsLoader;
import io.dagster.pipes.loaders.PipesParamsLoader;
import io.dagster.pipes.writers.PipesDefaultMessageWriter;
import io.dagster.pipes.writers.PipesMessageWriter;
import io.dagster.pipes.writers.PipesMessageWriterChannel;
import java.util.logging.Logger;

@SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.CommentSize"})
public class PipesSession {

    private final PipesContext context;
    private static final Logger LOGGER = Logger.getLogger(PipesSession.class.getName());

    /** Constructor. */
    public PipesSession(
        final PipesParamsLoader paramsLoader,
        final PipesContextLoader contextLoader,
        final PipesMessageWriter<? extends PipesMessageWriterChannel> messageWriter
    ) throws DagsterPipesException {
        this.context = buildContext(paramsLoader, contextLoader, messageWriter);
    }

    /**
     * Executes the provided runnable within the Dagster Pipes context,
     * handling lifecycle management and error reporting.
     *
     * <p>This method:
     * <ol>
     *   <li>Invokes the provided {@code runnable} with the initialized {@link PipesContext}.</li>
     *   <li>Captures any exception thrown by the runnable and reports it
     *       via {@link PipesContext#reportException(Exception)}.</li>
     *   <li>Ensures the {@link PipesContext} is properly closed via {@link PipesContext#close()}
     *       in a {@code finally} block, regardless of success or failure.</li>
     * </ol>
     *
     * <p><b>Important:</b> If this process was not launched by a Dagster orchestration
     * (determined during context initialization), the context will be a mock and all operations
     * (logging, reporting, etc.) will be no-ops.
     *
     * @param runnable The business logic to execute. Accepts a {@link PipesContext} parameter
     *     and may throw any {@link Exception}.
     * @throws DagsterPipesException Under two conditions:
     *     <ol>
     *       <li>If the context fails to initialize during session construction (thrown by the constructor).</li>
     *       <li>If an error occurs during context closure (e.g., failing to flush pending messages).</li>
     *     </ol>
     *     Note: Exceptions thrown by {@code runnable} are <i>not</i> propagated;
     *     they are captured and reported internally.
     */
    public void runDagsterPipes(final ThrowingConsumer runnable) throws DagsterPipesException {
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
        final PipesParamsLoader paramsLoader,
        final PipesContextLoader contextLoader,
        final PipesMessageWriter<? extends PipesMessageWriterChannel> messageWriter
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
            pipesContext = org.mockito.Mockito.mock(PipesContext.class);
        }
        PipesContext.set(pipesContext);
        return pipesContext;
    }

    private static void emitOrchestrationInactiveWarning() {
        LOGGER.warning(
            "This process was not launched by a Dagster orchestration process. All calls to the "
                + "`dagster-pipes` context or attempts to initialize "
                + "`dagster-pipes` abstractions are no-ops."
        );
    }
}
