package pipes.data;

public enum PipesConstants {

    CONTEXT_ENV_VAR("DAGSTER_PIPES_CONTEXT"),
    MESSAGES_ENV_VAR("DAGSTER_PIPES_MESSAGES"),
    PATH_KEY("path"),
    PIPES_PROTOCOL_VERSION_FIELD("__dagster_pipes_version"),
    /**
     * This represents the version of the protocol, rather than the version of the package.
     * It must be manually updated whenever there are changes to the protocol.
     */
    PIPES_PROTOCOL_VERSION("0.1");


    public final String name;

    PipesConstants(final String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
