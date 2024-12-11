package pipes.writers;

import pipes.DagsterPipesException;

import java.util.Map;

public abstract class PipesBlobStoreMessageWriter extends PipesMessageWriter<PipesMessageWriterChannel>{

    protected final float interval;

    public PipesBlobStoreMessageWriter() {
        super();
        this.interval = 1000;
    }

    public PipesBlobStoreMessageWriter(float interval) {
        super();
        this.interval = interval;
    }

    @Override
    public PipesMessageWriterChannel open(Map<String, Object> params) throws DagsterPipesException {
        PipesBlobStoreMessageWriterChannel writerChannel = this.makeChannel(params, this.interval);
        writerChannel.startBufferedUploadLoop();
        return writerChannel;
    }

    public abstract PipesBlobStoreMessageWriterChannel makeChannel(
        Map<String, Object> params, float interval
    ) throws DagsterPipesException;

}
