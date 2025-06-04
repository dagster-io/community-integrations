package io.dagster.pipes.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.data.PipesConstants;
import io.dagster.pipes.data.PipesMetadata;
import io.dagster.pipes.writers.PipesMessage;
import io.dagster.types.Method;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.zip.InflaterInputStream;

public final class PipesUtils {

    private PipesUtils() {
    }

    /**
     * Validates and retrieves a parameter from an environment map with expected type.
     *
     * @param <T>         Expected parameter type
     * @param envParams   Environment parameters map
     * @param key         Parameter key to retrieve
     * @param expectedType Class object representing expected type
     * @param cls         Calling class context for error reporting
     * @return Parameter value cast to expected type
     * @throws DagsterPipesException If parameter is missing, null, or has incorrect type
     */
    public static <T> T assertParamType(
        final Map<String, ?> envParams,
        final String key,
        final Class<T> expectedType,
        final Class<?> cls
    ) throws DagsterPipesException {
        final Object value = envParams.get(key);

        if (!expectedType.isInstance(value)) {
            throw new DagsterPipesException(
                String.format(
                    "Invalid type for parameter %s passed from orchestration side to %s. Expected %s, got %s.",
                    key,
                    cls.getSimpleName(),
                    expectedType.getSimpleName(),
                    value.getClass().getSimpleName()
                )
            );
        }

        return expectedType.cast(value);
    }

    /**
     * Constructs a standardized pipes message with protocol version.
     *
     * @param method The {@link Method} type
     * @param params Message payload parameters
     * @return The {@link PipesMessage}
     */
    public static PipesMessage makeMessage(final Method method, final Map<String, ?> params) {
        return new PipesMessage(PipesConstants.PIPES_PROTOCOL_VERSION.toString(), method.toValue(), params);
    }

    /**
     * Converts a metadata mapping to Dagster's internal format. Handles both:
     * <ul>
     *   <li>Raw values (auto-converted to {@link PipesMetadata})</li>
     *   <li>Pre-formed {@link PipesMetadata} objects</li>
     * </ul>
     *
     * @param <T>             Original metadata value type
     * @param metadataMapping Input metadata key-value pairs
     * @return Normalized metadata map with {@link PipesMetadata} values
     */
    public static <T> Map<String, PipesMetadata> resolveMetadataMapping(final Map<String, T> metadataMapping) {
        final boolean containsNonPipesMetadata = metadataMapping.values().stream()
            .anyMatch(value -> !(value instanceof PipesMetadata));

        return containsNonPipesMetadata
            ? MetadataBuilder.buildFrom(metadataMapping)
            : (Map<String, PipesMetadata>) metadataMapping;
    }

    /**
     * Decodes and decompresses a base64-encoded, zlib-compressed parameter string.
     *
     * @param rawValue Encoded parameter string (base64 + zlib)
     * @return Decoded parameter map
     * @throws DagsterPipesException If decompression or JSON parsing fails
     */
    public static Map<String, Object> decodeParam(final String rawValue) throws DagsterPipesException {
        try {
            final byte[] base64Decoded = Base64.getDecoder().decode(rawValue);
            final byte[] zlibDecompressed = zlibDecompress(base64Decoded);
            final ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(
                    zlibDecompressed,
                    new TypeReference<Map<String, Object>>() {}
            );
        } catch (IOException ioe) {
            throw new DagsterPipesException("Failed to decompress parameters", ioe);
        }
    }

    /**
     * Decompresses zlib-compressed byte data.
     *
     * @param data Compressed byte array
     * @return Decompressed byte array
     * @throws IOException If decompression fails
     */
    public static byte[] zlibDecompress(final byte[] data) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            InflaterInputStream filterStream = new InflaterInputStream(inputStream);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            final byte[] buffer = new byte[1024];
            int readChunk;

            while (true) {
                readChunk = filterStream.read(buffer);
                if (readChunk == -1) {
                    break;
                }
                outputStream.write(buffer, 0, readChunk);
            }

            return outputStream.toByteArray();
        }
    }
}
