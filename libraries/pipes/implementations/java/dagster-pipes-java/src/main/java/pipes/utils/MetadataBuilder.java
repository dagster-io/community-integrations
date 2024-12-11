package pipes.utils;

import pipes.data.PipesMetadata;
import types.Type;

import java.util.*;
import java.util.function.Function;

public class MetadataBuilder {

    private static final Map<Class<?>, Function<Object, Map<String, PipesMetadata>>> TYPE_ACTIONS
        = new HashMap<>();

    static {
        TYPE_ACTIONS.put(Float.class, object -> buildMetadata(object, "float", Type.FLOAT));
        TYPE_ACTIONS.put(Integer.class, object -> buildMetadata(object, "int", Type.INT));
        TYPE_ACTIONS.put(String.class, object -> buildMetadata(object, "text", Type.TEXT));
        TYPE_ACTIONS.put(Boolean.class, object -> buildMetadata(object, "boolean", Type.BOOL));
        TYPE_ACTIONS.put(Map.class, object -> buildMetadata(object, "json", Type.JSON));
        TYPE_ACTIONS.put(List.class, object -> buildMetadata(object, "json", Type.JSON));
    }

    public static Map<String, PipesMetadata> buildFrom(final Object object) {
        if (object == null) {
            throw new IllegalArgumentException(
                "Automatic metadata builder doesn't support null values!"
            );
        }

        if (object.getClass().isArray()) {
            return buildMetadata(object, "json", Type.JSON);
        }

        Function<Object, Map<String, PipesMetadata>> action = null;
        for (final Map.Entry<Class<?>, Function<Object, Map<String, PipesMetadata>>> entry:
            TYPE_ACTIONS.entrySet()
        ) {
            if (entry.getKey().isAssignableFrom(object.getClass())) {
                action = entry.getValue();
                break;
            }
        }

        if (action == null) {
            throw new UnsupportedOperationException(
                String.format(
                    "Automatic metadata builder doesn't support: %s",
                    object.getClass().getName()
                )
            );
        }

        return action.apply(object);
    }

    private static Map<String, PipesMetadata> buildMetadata(
        final Object object, final String key, final Type type
    ) {
        final Map<String, PipesMetadata> metadataMap = new HashMap<>();
        metadataMap.put(key, new PipesMetadata(object, type));
        return metadataMap;
    }
}
