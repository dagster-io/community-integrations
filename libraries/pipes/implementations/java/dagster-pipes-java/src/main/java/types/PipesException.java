package types;

import com.fasterxml.jackson.annotation.*;
import java.util.List;

public class PipesException {
    private PipesExceptionClass cause;
    private ContextClass context;
    private String message;
    private String name;
    private List<String> stack;

    /**
     * exception that explicitly led to this exception
     */
    @JsonProperty("cause")
    public PipesExceptionClass getCause() { return cause; }
    @JsonProperty("cause")
    public void setCause(PipesExceptionClass value) { this.cause = value; }

    /**
     * exception that being handled when this exception was raised
     */
    @JsonProperty("context")
    public ContextClass getContext() { return context; }
    @JsonProperty("context")
    public void setContext(ContextClass value) { this.context = value; }

    @JsonProperty("message")
    public String getMessage() { return message; }
    @JsonProperty("message")
    public void setMessage(String value) { this.message = value; }

    /**
     * class name of Exception object
     */
    @JsonProperty("name")
    public String getName() { return name; }
    @JsonProperty("name")
    public void setName(String value) { this.name = value; }

    @JsonProperty("stack")
    public List<String> getStack() { return stack; }
    @JsonProperty("stack")
    public void setStack(List<String> value) { this.stack = value; }
}
