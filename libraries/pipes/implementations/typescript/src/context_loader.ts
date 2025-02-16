import * as fs from "fs";
import { PipesContextData } from "./types";
import { DagsterPipesError } from "./types";

/**
 * A context loader loads context data injected by the orchestration process.
 */
export abstract class PipesContextLoader {
    public constructor() {}
    
    /**
     * This method should read and yield the context data from the location specified by the passed in
     * `PipesParams`.
     *
     * @param contextParams - The params provided by the context injector in the orchestration process.
     * @returns The loaded context data.
     */
    public abstract loadContext(contextParams: Record<string, any>): PipesContextData;
}

/**
 * Context loader that loads context data from either a file or directly from the provided params.
 *
 * The location of the context data is configured by the params received by the loader. If the params
 * include a key `path`, then the context data will be loaded from a file at the specified path. If
 * the params instead include a key `data`, then the corresponding value should be a dict
 * representing the context data.
 */
export class PipesDefaultContextLoader extends PipesContextLoader {
    public constructor() {
        super();
    }

    public loadContext(contextParams: Record<string, any>): PipesContextData {
        if ("path" in contextParams) {
            const contextFilePath = contextParams["path"] as string;
            return JSON.parse(fs.readFileSync(contextFilePath, "utf8"));
        } else if ("data" in contextParams) {
            return contextParams["data"];
        } else {
            throw new DagsterPipesError(`context params must contain either 'path' or 'data' key - but received ${JSON.stringify(contextParams)}`);
        }
    }
}