package io.dagster.pipes.loaders;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.data.PipesContextData;
import io.dagster.pipes.utils.PipesUtils;
import java.io.IOException;
import java.util.Map;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class PipesS3ContextLoader extends PipesContextLoader {

    private final S3Client client;

    public PipesS3ContextLoader(final S3Client client) {
        super();
        this.client = client;
    }

    @Override
    public PipesContextData loadContext(final Map<String, Object> params) throws DagsterPipesException {
        final String bucket = PipesUtils.assertParamType(params, "bucket", String.class, PipesS3ContextLoader.class);
        final String key = PipesUtils.assertParamType(params, "key", String.class, PipesS3ContextLoader.class);
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .build();
        try (ResponseInputStream<GetObjectResponse> inputStream = client.getObject(getObjectRequest)) {
            final ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(inputStream, PipesContextData.class);
        } catch (IOException ioe) {
            throw new DagsterPipesException("Failed to load S3 object content!", ioe);
        }
    }

}
