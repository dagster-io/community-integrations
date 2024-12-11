package pipes;

public class DagsterPipesException extends Exception {

    private static final long serialVersionUID = 1451288228997568178L;

    public DagsterPipesException(String message) {
        super(message);
    }

    public DagsterPipesException(String message, Throwable cause) {
        super(message, cause);
    }

}
