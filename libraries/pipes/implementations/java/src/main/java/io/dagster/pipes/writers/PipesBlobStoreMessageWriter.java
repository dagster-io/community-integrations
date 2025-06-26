package io.dagster.pipes.writers;

import io.dagster.pipes.DagsterPipesException;
import java.util.Map;

public abstract class PipesBlobStoreMessageWriter extends PipesMessageWriter<PipesMessageWriterChannel> {

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
        final PipesBlobStoreMessageWriterChannel writerChannel = this.makeChannel(params, this.interval);
        writerChannel.startBufferedUploadLoop();
        return writerChannel;
    }

    public abstract PipesBlobStoreMessageWriterChannel makeChannel(
        Map<String, Object> params,
        float interval
    ) throws DagsterPipesException;

}
