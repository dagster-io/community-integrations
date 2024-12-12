package pipes.utils;

import pipes.DagsterPipesException;
import pipes.data.PipesConstants;
import pipes.data.PipesMetadata;
import pipes.writers.PipesMessage;
import types.Method;

import java.util.Map;

public final class PipesUtils {

    private PipesUtils() {
    }

    public static <T> T assertParamType(
        Map<String, ?> envParams,
        String key,
        Class<T> expectedType,
        Class<?> cls
    ) throws DagsterPipesException {
        Object value = envParams.get(key);

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

    public static PipesMessage makeMessage(Method method, Map<String, ?> params) {
        return new PipesMessage(PipesConstants.PIPES_PROTOCOL_VERSION.toString(), method.toValue(), params);
    }

    public static <T> Map<String, PipesMetadata> resolveMetadataMapping(final Map<String, T> metadataMapping) {
        boolean containsNonPipesMetadata = metadataMapping.values().stream()
            .anyMatch(value -> !(value instanceof PipesMetadata));

        return containsNonPipesMetadata
            ? MetadataBuilder.buildFrom(metadataMapping)
            : (Map<String, PipesMetadata>) metadataMapping;
    }
}
