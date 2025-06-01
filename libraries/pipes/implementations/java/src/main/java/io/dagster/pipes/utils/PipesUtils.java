package io.dagster.pipes.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.zip.InflaterInputStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.data.PipesConstants;
import io.dagster.pipes.data.PipesMetadata;
import io.dagster.pipes.writers.PipesMessage;
import io.dagster.types.Method;

public final class PipesUtils {

    private PipesUtils() {
    }

    public static <T> T assertParamType(
        final Map<String, ?> envParams,
        final String key,
        final Class<T> expectedType,
        final Class<?> cls
    ) throws DagsterPipesException {
        final Object value = envParams.get(key);

        if (!expectedType.isInstance(value)) {
            throw new DagsterPipesException (
                String.format(
                    "Invalid type for parameter %s passed from orchestration side to %s." +
                    "\nExpected %s, got %s.",
                    key,
                    cls.getSimpleName(),
                    expectedType.getSimpleName(),
                    value.getClass().getSimpleName()
                )
            );
        }

        return expectedType.cast(value);
    }

    public static PipesMessage makeMessage(final Method method, final Map<String, ?> params) {
        return new PipesMessage(PipesConstants.PIPES_PROTOCOL_VERSION.toString(), method.toValue(), params);
    }

    public static <T> Map<String, PipesMetadata> resolveMetadataMapping(final Map<String, T> metadataMapping) {
        final boolean containsNonPipesMetadata = metadataMapping.values().stream()
            .anyMatch(value -> !(value instanceof PipesMetadata));

        return containsNonPipesMetadata
            ? MetadataBuilder.buildFrom(metadataMapping)
            : (Map<String, PipesMetadata>) metadataMapping;
    }

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

    public static byte[] zlibDecompress(final byte[] data) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
             InflaterInputStream filterStream = new InflaterInputStream(inputStream);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            final byte[] buffer = new byte[1024];
            int readChunk;

            while ((readChunk = filterStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, readChunk);
            }

            return outputStream.toByteArray();
        }
    }
}
