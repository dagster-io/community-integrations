package io.dagster.pipes.writers;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.utils.PipesUtils;
import java.util.Map;
import software.amazon.awssdk.services.s3.S3Client;

public class PipesS3MessageWriter extends PipesBlobStoreMessageWriter {

    private final S3Client client;

    public PipesS3MessageWriter(final S3Client client) {
        super(1000);
        this.client = client;
    }

    /**
     * Message writer that writes messages by periodically writing message chunks to an S3 bucket.
     *
     * @param client   An object representing the S3 client.
     * @param interval Interval in seconds between upload chunk uploads.
     */
    public PipesS3MessageWriter(final S3Client client, final long interval) {
        super(interval);
        this.client = client;
    }

    /**
     * Creates a new S3 message writer channel.
     *
     * @param params Parameters required for creating the channel.
     * @return A new instance of {@link PipesS3MessageWriterChannel}.
     */
    @Override
    public PipesS3MessageWriterChannel makeChannel(
        final Map<String, Object> params,
        final float interval
    ) throws DagsterPipesException {
        final String bucket = PipesUtils.assertParamType(
            params, "bucket", String.class, PipesS3MessageWriter.class
        );
        final String keyPrefix = PipesUtils.assertParamType(
            params, "key_prefix", String.class, PipesS3MessageWriter.class
        );
        return new PipesS3MessageWriterChannel(client, bucket, keyPrefix, super.interval);
    }
}
