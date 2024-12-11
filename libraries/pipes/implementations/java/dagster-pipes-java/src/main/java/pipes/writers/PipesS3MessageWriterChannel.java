package pipes.writers;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.StringWriter;

public class PipesS3MessageWriterChannel extends PipesBlobStoreMessageWriterChannel {

    private final S3Client client;
    private final String bucket;
    private final String keyPrefix;

    /**
     * Constructs a message writer channel for writing messages to an S3 bucket.
     *
     * @param client    The S3 client object.
     * @param bucket    The name of the S3 bucket.
     * @param keyPrefix An optional prefix for the keys of written blobs.
     * @param interval  The interval in seconds between upload chunk uploads.
     */
    public PipesS3MessageWriterChannel(S3Client client, String bucket, String keyPrefix, float interval) {
        super(interval);
        this.client = client;
        this.bucket = bucket;
        this.keyPrefix = keyPrefix;
    }

    /**
     * Uploads a chunk of messages to the S3 bucket.
     *
     * @param payload The payload to upload as a StringWriter.
     * @param index   The index used to construct the S3 key.
     */
    @Override
    protected void uploadMessagesChunk(StringWriter payload, int index) {
        String key = keyPrefix != null ? keyPrefix + "/" + index + ".json" : index + ".json";
        String content = payload.toString();

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .contentType("application/json")
            .build();

        client.putObject(putObjectRequest, RequestBody.fromString(content));
    }
}
