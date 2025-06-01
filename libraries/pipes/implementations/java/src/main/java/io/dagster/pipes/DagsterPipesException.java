package io.dagster.pipes;

public class DagsterPipesException extends Exception {

    private static final long serialVersionUID = 1451288228997568178L;

    public DagsterPipesException(final String message) {
        super(message);
    }

    public DagsterPipesException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
