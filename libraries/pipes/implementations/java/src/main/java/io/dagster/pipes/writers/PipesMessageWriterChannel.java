package io.dagster.pipes.writers;

import io.dagster.pipes.DagsterPipesException;

public interface PipesMessageWriterChannel {

    void writeMessage(PipesMessage message) throws DagsterPipesException;

    void close() throws DagsterPipesException;
}
