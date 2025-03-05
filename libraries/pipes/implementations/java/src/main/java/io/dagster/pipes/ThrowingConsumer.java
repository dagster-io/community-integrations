package io.dagster.pipes;

public interface ThrowingConsumer {
    void run(PipesContext context) throws DagsterPipesException;
}
