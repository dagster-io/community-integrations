package io.dagster.pipes.writers;

import java.util.Map;

import io.dagster.pipes.DagsterPipesException;

public abstract class PipesBlobStoreMessageWriter extends PipesMessageWriter<PipesMessageWriterChannel>{

    protected final float interval;

    public PipesBlobStoreMessageWriter() {
        super();
        this.interval = 1000;
    }

    public PipesBlobStoreMessageWriter(final float interval) {
        super();
        this.interval = interval;
    }

    @Override
    public PipesMessageWriterChannel open(final Map<String, Object> params) throws DagsterPipesException {
        PipesBlobStoreMessageWriterChannel writerChannel = this.makeChannel(params, this.interval);
        writerChannel.startBufferedUploadLoop();
        return writerChannel;
    }

    public abstract PipesBlobStoreMessageWriterChannel makeChannel(
        final Map<String, Object> params, final float interval
    ) throws DagsterPipesException;

}
