package pipes.writers;

import pipes.DagsterPipesException;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;

import static pipes.utils.PipesUtils.assertParamType;

public class PipesS3MessageWriter extends PipesBlobStoreMessageWriter {

    private final S3Client client;

    public PipesS3MessageWriter(S3Client client) {
        super(1000);
        this.client = client;
    }

    /**
     * Message writer that writes messages by periodically writing message chunks to an S3 bucket.
     *
     * @param client   An object representing the S3 client.
     * @param interval Interval in seconds between upload chunk uploads.
     */
    public PipesS3MessageWriter(S3Client client, long interval) {
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
    public PipesS3MessageWriterChannel makeChannel(Map<String, Object> params, float interval) throws DagsterPipesException {
        String bucket = assertParamType(params, "bucket", String.class, PipesS3MessageWriter.class);
        String keyPrefix = assertParamType(params, "key_prefix", String.class, PipesS3MessageWriter.class);
        return new PipesS3MessageWriterChannel(client, bucket, keyPrefix, super.interval);
    }
}
