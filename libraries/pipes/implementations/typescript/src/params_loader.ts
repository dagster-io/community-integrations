import * as zlib from "zlib";

const DAGSTER_PIPES_CONTEXT_ENV_VAR = "DAGSTER_PIPES_CONTEXT";
const DAGSTER_PIPES_MESSAGES_ENV_VAR = "DAGSTER_PIPES_MESSAGES";

/**
 * Object that loads params passed from the orchestration process by the context injector and
 * message reader. These params are used to respectively bootstrap the
 * `PipesContextLoader` and `PipesMessageWriter`.
 */
export abstract class PipesParamsLoader {
    public constructor() {}

    /**
     * Load params passed by the orchestration-side context injector.
     *
     * @returns The loaded context params.
     */
    public abstract loadContextParams(): Record<string, any>;

    /**
     * Load params passed by the orchestration-side message reader.
     *
     * @returns The loaded message params.
     */
    public abstract loadMessagesParams(): Record<string, any>;

    /**
     * Whether or not this process has been provided with information to create
     * a PipesContext or should instead return a mock.
     *
     * @returns True if the process is a Dagster Pipes process, false otherwise.
     */
    public abstract isDagsterPipesProcess(): boolean;
}

/**
 * Params loader that extracts params from environment variables.
 */
export class PipesEnvParamsLoader extends PipesParamsLoader {
    private env: NodeJS.ProcessEnv;

    public constructor() {
        super();
        this.env = process.env;
    }

    public loadContextParams(): Record<string, any> {
        const encodedContextParam = this.env[DAGSTER_PIPES_CONTEXT_ENV_VAR] as string;
        const buff = Buffer.from(encodedContextParam, "base64");
        return JSON.parse(zlib.inflateSync(buff).toString());
    }

    public loadMessagesParams(): Record<string, any> {
        const encodedMessagesParam = this.env[DAGSTER_PIPES_MESSAGES_ENV_VAR] as string;
        const buff = Buffer.from(encodedMessagesParam, "base64");
        return JSON.parse(zlib.inflateSync(buff).toString());
    }

    public isDagsterPipesProcess(): boolean {
        return DAGSTER_PIPES_CONTEXT_ENV_VAR in this.env;
    }
}