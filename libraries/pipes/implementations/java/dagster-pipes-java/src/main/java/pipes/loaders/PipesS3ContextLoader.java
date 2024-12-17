package pipes.loaders;

import com.fasterxml.jackson.databind.ObjectMapper;
import pipes.DagsterPipesException;
import pipes.data.PipesContextData;
import pipes.utils.PipesUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.util.Map;

public class PipesS3ContextLoader extends PipesContextLoader {

    private final S3Client client;

    public PipesS3ContextLoader(S3Client client) {
        super();
        this.client = client;
    }

    @Override
    public PipesContextData loadContext(Map<String, Object> params) throws DagsterPipesException {
        String bucket = PipesUtils.assertParamType(params, "bucket", String.class, PipesS3ContextLoader.class);
        String key = PipesUtils.assertParamType(params, "key", String.class, PipesS3ContextLoader.class);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .build();
        try (ResponseInputStream<GetObjectResponse> inputStream = client.getObject(getObjectRequest)) {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(inputStream, PipesContextData.class);
        } catch (IOException ioe) {
            throw new DagsterPipesException("Failed to load S3 object content!", ioe);
        }
    }

}
