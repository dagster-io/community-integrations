package io.dagster.pipes.utils;

import io.dagster.pipes.data.PipesMetadata;
import io.dagster.types.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MetadataBuilder {

    private static final Map<Class<?>, Function<Map.Entry<String, ?>, Map<String, PipesMetadata>>>
        TYPE_ACTIONS = new HashMap<>();

    static {
        TYPE_ACTIONS.put(Float.class, entry -> buildMetadata(entry, Type.FLOAT));
        TYPE_ACTIONS.put(Integer.class, entry -> buildMetadata(entry, Type.INT));
        TYPE_ACTIONS.put(String.class, entry -> buildMetadata(entry, Type.TEXT));
        TYPE_ACTIONS.put(Boolean.class, entry -> buildMetadata(entry, Type.BOOL));
        TYPE_ACTIONS.put(Map.class, entry -> buildMetadata(entry, Type.JSON));
        TYPE_ACTIONS.put(List.class, entry -> buildMetadata(entry, Type.JSON));
    }

    /**
     * Builds a metadata map from a generic key-value mapping by processing each entry.
     * <p>
     * This method iterates through each entry in the input map and converts it to one or more
     * {@link PipesMetadata} entries using type-specific handlers registered in {@code TYPE_ACTIONS}.
     * Processing stops immediately if an array value is encountered (after handling that array).
     * </p>
     *
     * <p><b>Important behaviors:</b>
     * <ul>
     *   <li>Null values are strictly prohibited</li>
     *   <li>Array values trigger special handling and terminate processing immediately</li>
     *   <li>Values must be of a type supported by registered handlers in {@code TYPE_ACTIONS}</li>
     * </ul>
     * </p>
     *
     * @param mapping Source map containing metadata keys and values. Values must be non-null
     *                and of a supported type (as defined in {@code TYPE_ACTIONS}).
     * @return New map containing processed {@code PipesMetadata} entries
     */
    public static Map<String, PipesMetadata> buildFrom(final Map<String, ?> mapping) {
        final Map<String, PipesMetadata> result = new HashMap<>();
        for (final Map.Entry<String, ?> entry: mapping.entrySet()) {
            final Object value = entry.getValue();
            if (value == null) {
                throw new IllegalArgumentException(
                    "Automatic metadata builder doesn't support null values!"
                );
            }

            if (value.getClass().isArray()) {
                result.putAll(buildMetadata(value, entry.getKey(), Type.JSON));
                break;
            }

            Function<Map.Entry<String, ?>, Map<String, PipesMetadata>> action = null;
            for (final Map.Entry<Class<?>, Function<Map.Entry<String, ?>, Map<String, PipesMetadata>>> actionEntry:
                TYPE_ACTIONS.entrySet()
            ) {
                if (actionEntry.getKey().isAssignableFrom(entry.getValue().getClass())) {
                    action = actionEntry.getValue();
                    break;
                }
            }

            if (action == null) {
                throw new UnsupportedOperationException(
                    String.format(
                        "Automatic metadata builder doesn't support: %s",
                        mapping.getClass().getName()
                    )
                );
            }

            result.putAll(action.apply(entry));
        }
        return result;
    }

    private static Map<String, PipesMetadata> buildMetadata(
        final Map.Entry<String, ?> entry, final Type type
    ) {
        return buildMetadata(entry.getValue(), entry.getKey(), type);
    }

    private static Map<String, PipesMetadata> buildMetadata(
        final Object object, final String key, final Type type
    ) {
        final Map<String, PipesMetadata> metadataMap = new HashMap<>();
        metadataMap.put(key, new PipesMetadata(object, type));
        return metadataMap;
    }
}
