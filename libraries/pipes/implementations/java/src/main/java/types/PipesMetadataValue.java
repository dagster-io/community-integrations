package types;

import com.fasterxml.jackson.annotation.*;
import java.util.Map;

public class PipesMetadataValue {
    private RawValue rawValue;
    private Type type;

    @JsonProperty("raw_value")
    public RawValue getRawValue() { return rawValue; }
    @JsonProperty("raw_value")
    public void setRawValue(RawValue value) { this.rawValue = value; }

    @JsonProperty("type")
    public Type getType() { return type; }
    @JsonProperty("type")
    public void setType(Type value) { this.type = value; }
}
