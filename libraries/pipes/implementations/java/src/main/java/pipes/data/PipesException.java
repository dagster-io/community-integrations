package pipes.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Arrays;

@SuppressWarnings("PMD")
@JsonPropertyOrder({"name", "message", "cause", "stack", "context"})
public class PipesException {
    private PipesException cause;
    private String message;
    private String name;
    private String[] stack;
    private static final String[] CONTEXT = {};

    public PipesException(Throwable exception) {
        this(exception, true);
    }

    private PipesException(Throwable exception, boolean withStackTrace) {
        this.cause = exception.getCause() == null ? null : new PipesException(exception.getCause(), false);
        this.message = exception.getMessage();
        this.name = exception.getClass().getTypeName();
        if (withStackTrace) {
            this.stack = Arrays
                .stream(exception.getStackTrace())
                .map(StackTraceElement::toString)
                .toArray(String[]::new);
        }
    }

    @JsonProperty("cause")
    public PipesException getCause() { return cause; }
    @JsonProperty("cause")
    public void setCause(PipesException value) { this.cause = value; }

    @JsonProperty("message")
    public String getMessage() { return message; }
    @JsonProperty("message")
    public void setMessage(String value) { this.message = value; }

    @JsonProperty("name")
    public String getName() { return name; }
    @JsonProperty("name")
    public void setName(String value) { this.name = value; }

    @JsonProperty("stack")
    public String[] getStack() { return stack; }
    @JsonProperty("stack")
    public void setStack(String[] value) { this.stack = value; }

    @JsonProperty("context")
    public String[] getContext() { return CONTEXT; }
}
