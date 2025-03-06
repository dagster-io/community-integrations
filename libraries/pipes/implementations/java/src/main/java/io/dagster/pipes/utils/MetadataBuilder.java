package io.dagster.pipes.utils;

import java.util.*;
import java.util.function.Function;

import io.dagster.pipes.data.PipesMetadata;
import io.dagster.types.Type;

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
