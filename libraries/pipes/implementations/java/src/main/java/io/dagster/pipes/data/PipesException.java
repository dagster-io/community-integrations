package io.dagster.pipes.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Arrays;

@JsonPropertyOrder({"name", "message", "cause", "stack", "context"})
@SuppressWarnings("PMD.DataClass")
public class PipesException {
    private PipesException cause;
    private String message;
    private String name;
    private String[] stack;
    private static final String[] CONTEXT = {};

    public PipesException(final Throwable exception) {
        this(exception, true);
    }

    private PipesException(final Throwable exception, final boolean withStackTrace) {
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
    public PipesException getCause() {
        return cause;
    }

    @JsonProperty("cause")
    public void setCause(final PipesException value) {
        this.cause = value;
    }

    @JsonProperty("message")
    public String getMessage() {
        return message;
    }

    @JsonProperty("message")
    public void setMessage(final String value) {
        this.message = value;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(final String value) {
        this.name = value;
    }

    @JsonProperty("stack")
    public String[] getStack() {
        return this.stack == null ? null : Arrays.copyOf(this.stack, this.stack.length);
    }

    @JsonProperty("stack")
    public void setStack(final String... value) {
        this.stack = value.clone();
    }

    @JsonProperty("context")
    public String[] getContext() {
        return CONTEXT;
    }
}
