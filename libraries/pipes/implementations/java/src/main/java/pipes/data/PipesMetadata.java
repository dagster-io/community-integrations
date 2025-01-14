package pipes.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import types.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SuppressWarnings("PMD")
@JsonPropertyOrder({"raw_value", "type"})
public class PipesMetadata {

    @JsonProperty("raw_value")
    private Object rawValue;
    @JsonProperty("type")
    private Type type;

    public static final List<Class<?>> ALLOWED_VALUE_TYPES = Arrays.asList(
        Float.class, Integer.class, Long.class, Double.class,
        Map.class, Boolean.class, String.class, List.class
    );

    public PipesMetadata(Object value, Type type) {
        if (value != null && ALLOWED_VALUE_TYPES.stream().noneMatch(vt -> vt.isInstance(value)) && !value.getClass().isArray()) {
            throw new IllegalArgumentException(String.format(
                "Wrong metadata value type: %s", value.getClass().getTypeName()
            ));
        }
        if (value instanceof Map && ((Map<?, ?>) value).keySet().stream().anyMatch(k -> !(k instanceof String))) {
            throw new IllegalArgumentException(String.format(
                "Wrong metadata value map type. Only String keys allowed: %s", value.getClass().getTypeName()
            ));
        }
        this.rawValue = value;
        this.type = type;
    }

    public Object getRawValue() {
        return rawValue;
    }

    public Type getType() {
        return type;
    }
}
