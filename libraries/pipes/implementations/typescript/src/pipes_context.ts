import { PipesEnvParamsLoader, PipesParamsLoader } from "./params_loader";
import { PipesContextLoader, PipesDefaultContextLoader } from "./context_loader";
import { PipesDataProvenance, PipesException, PipesMessage, PipesPartitionKeyRange, PipesTimeWindow } from "./types";
import { PipesContextData } from "./types";
import { normalizeMetadata } from "./normalize_metadata";
import { PipesDefaultMessageWriter, PipesMessageWriter } from "./message_writer";
import { PipesLogger } from "./logger";
import { DagsterPipesError } from "./types";

export const PIPES_PROTOCOL_VERSION = "0.1"

/**
 * Initialize the Dagster Pipes context.
 *
 * This function should be called near the entry point of a pipes process. It will load injected
 * context information from Dagster and spin up the machinery for streaming messages back to
 * Dagster.
 *
 * If the process was not launched by Dagster, this function will emit a warning and return a
 * "no-op" object - in which all method calls are a no-op.
 * 
 * The returned PipesContext object is disposable - so it is recomended to use the `using` pattern,
 * to make sure the context is disposed at the end.
 * `using context = openDagsterPipes()`
 * 
 *
 * @param paramsLoader - The params loader to use. Defaults to `PipesEnvVarParamsLoader`.
 * @param contextLoader - The context loader to use. Defaults to `PipesDefaultContextLoader`.
 * @param messageWriter - The message writer to use. Defaults to `PipesDefaultMessageWriter`.
 * @returns The initialized context.
 */
export function openDagsterPipes(
    paramsLoader: PipesParamsLoader | null = null, 
    contextLoader: PipesContextLoader | null = null,
    messageWriter: PipesMessageWriter | null = null): PipesContext {
    return PipesContext.getInstance(paramsLoader, contextLoader, messageWriter);
}


/**
 * The context for a Dagster Pipes process.
 *
 * This class should not be instantiated by the user - who should rather use the `openDagsterPipes()` function. 
 * 
 * PipesContext is the abstract-base-class for `PipesContextImpl`. It also functions as a "no-op" class,
 * for the cases when the typescript process is not launched by the dagster orchestrator. In that case,
 * All the methods will act as no-ops, allowing the user's process to keep running as usual.
 * When the process is opened by the dagster orchestrator, a `PipesContextImpl` will be returned from
 * `openDagsterPipes` instead.
 * 
 * This class is analogous to `dagster.OpExecutionContext` on the Dagster side of the Pipes
 * connection. It provides access to information such as the asset key(s) and partition key(s) in
 * scope for the current step. It also provides methods for logging and emitting results that will
 * be streamed back to Dagster.
 */
/* eslint-disable @typescript-eslint/no-unused-vars*/
export class PipesContext {
    private static instance: PipesContext | null = null;

    private _logger: PipesLogger;

    public static getInstance(
        paramsLoader: PipesParamsLoader | null, 
        contextLoader: PipesContextLoader | null,
        messageWriter: PipesMessageWriter | null    
    ): PipesContext {
        if (PipesContext.instance) {
            return PipesContext.instance
        }

        if (!paramsLoader) {
            paramsLoader = new PipesEnvParamsLoader();
        }

        if (!paramsLoader.isDagsterPipesProcess()) {
            console.warn("This process was not launched by a Dagster orchestration process. All calls to the `dagster-pipes` context or attempts to initialize `dagster-pipes` abstractions are no-ops.")
            PipesContext.instance = new PipesContext(); // the base class functions as a no-op / mock.
            return PipesContext.instance; 
        }
        
        if (!contextLoader) {
            contextLoader = new PipesDefaultContextLoader();
        }

        if (!messageWriter) {
            messageWriter = new PipesDefaultMessageWriter();
        }

        PipesContext.instance = new PipesContextImpl(paramsLoader, contextLoader, messageWriter);
        return PipesContext.instance;
    }
    
    protected constructor() {
        this._logger = new PipesLogger(this);
    }

    /**
     * Report to Dagster that an asset has been materialized. Streams a payload containing
     * materialization information back to Dagster. If no assets are in scope, raises an error.
     *
     * @param metadata - Metadata for the materialized asset. Defaults to None.
     * @param dataVersion - The data version for the materialized asset. Defaults to None.
     * @param assetKey - The asset key for the materialized asset. If only a single asset is in scope,
     * this must be set explicitly or an error will be raised.
     */
    public reportAssetMaterialization(
        metadata: Record<string, any> | null = null,
        dataVersion: string | null = null,
        assetKey: string | null = null
    ): void {
        // no-op
    }

    /**
     * Report to Dagster that an asset check has been performed. Streams a payload containing
     * check result information back to Dagster. If no assets or associated checks are in scope, raises an error.
     *
     * @param checkName - The name of the check.
     * @param passed - Whether the check passed.
     * @param severity - The severity of the check. Defaults to "ERROR".
     * @param metadata - Metadata for the check. Defaults to None.
     * @param assetKey - The asset key for the check. If only a single asset is in scope, this must be
     * set explicitly or an error will be raised.
     */
    public reportAssetCheck(
        checkName: string,
        passed: boolean,
        severity: string = "ERROR",
        metadata: Record<string, any> | null = null,
        assetKey: string | null = null
    ): void {
        // no-op
    }
    
    /**
     * A logger that streams log messages back to Dagster.
     */
    public get logger(): PipesLogger {
        return this._logger;
    }

    /**
     * Log a message with a specified level.
     *
     * @param message - The message to log.
     * @param level - The level of the log message.
     */
    public log(message: string, level: string): void {
        // no-op
    }

    /**
     * Close the pipes connection. This will flush all buffered messages to the orchestration
     * process and cause any further attempt to write a message to raise an error. This method is
     * idempotent-- subsequent calls after the first have no effect.
     *
     * @param exception - An optional exception to report when closing.
     */
    public close(exception: string | PipesException | null = null): void {
        // no-op
    }

    public [Symbol.dispose]() {}

    /**
     * The AssetKey for the currently scoped asset. Raises an error if 0 or multiple assets
     * are in scope.
     */
    public get assetKey(): string {
        return "";
    }

    /**
     * The AssetKeys for the currently scoped assets. Raises an error if no
     * assets are in scope.
     */
    public get assetKeys(): string[] {
        return [];
    }

    /**
     * The provenance for the currently scoped asset. Raises an error if 0 or multiple assets are in scope.
     */
    public get provenance(): PipesDataProvenance | null {
        return null;
    }

    /**
     * Mapping of asset key to provenance for the currently scoped assets. Raises an error if no assets are in scope.
     */
    public get provenanceByAssetKey(): Record<string, PipesDataProvenance | null> {
        return {};
    }

    /**
     * The code version for the currently scoped asset. Raises an error if 0 or multiple assets are in scope.
     */
    public get codeVersion(): string | null {
        return null;
    }

    /**
     * Mapping of asset key to code version for the currently scoped assets. Raises an error if no assets are in scope.
     */
    public get codeVersionByAssetKey(): Record<string, string | null> {
        return {};
    }

    /**
     * Whether the current step is scoped to one or more partitions.
     */
    public get isPartitionStep(): boolean {
        return false;
    }

    /**
     * The partition key for the currently scoped partition. Raises an error if 0 or multiple partitions are in scope.
     */
    public get partitionKey(): string {
        return "";
    }

    /**
     * The partition key range for the currently scoped partition or partitions. Raises an error if no partitions are in scope.
     */
    public get partitionKeyRange(): PipesPartitionKeyRange {
        return { start: "", end: "" };
    }

    /**
     * The partition time window for the currently scoped partition or partitions. Returns null if partitions in scope are not temporal. Raises an error if no partitions are in scope.
     */
    public get partitionTimeWindow(): PipesTimeWindow | null {
        return null;
    }

    /**
     * The run ID for the currently executing pipeline run.
     */
    public get runID(): string {
        return "";
    }

    /**
     * The job name for the currently executing run. Returns null if the run is not derived from a job.
     */
    public get jobName(): string | null {
        return null;
    }

    /**
     * The retry number for the currently executing run.
     */
    public get retryNumber(): number {
        return 0;
    }

    /**
     * Get the value of an extra provided by the user. Raises an error if the extra is not defined.
     *
     * @param key - The key of the extra.
     * @returns The value of the extra.
     */
    public getExtra(key: string): any {
        return null;
    }

    /**
     * Key-value map for all extras provided by the user.
     */
    public get extras(): Record<string, any> {
        return {};
    }

    /**
     * Whether the context has been closed.
     */
    public get isClosed(): boolean {
        return false;
    }
}
/* eslint-enable @typescript-eslint/no-unused-vars*/

/* The actual implementation of the PipesContext abstract base class.
 * 
 * Not directly created by the user (who uses openDagsterPipes method).
 * All methods documented at the PipesContext base class.s
 */
class PipesContextImpl extends PipesContext {
    private contextData: PipesContextData;
    private messageWriter: PipesMessageWriter;

    private closed: boolean;
    private materializedAssetsList: string[];
    
    public constructor(paramsLoader: PipesParamsLoader, contextLoader: PipesContextLoader, messageWriter: PipesMessageWriter) {
        super();
        
        this.closed = false;
        this.materializedAssetsList = []

        // CONTEXT DATA
        const contextParams = paramsLoader.loadContextParams();
        this.contextData = contextLoader.loadContext(contextParams);

        // MESSAGE DATA
        const decodedMessageParams = paramsLoader.loadMessagesParams();
        this.messageWriter = messageWriter;
        this.messageWriter.open(decodedMessageParams)

        this.writeMessage("opened", {
            "extras": this.messageWriter.openedExtras()
        })
    }

    public close(exception: PipesException | string | null = null) {
        if (this.closed) {
            return;
        }

        if (typeof(exception) === "string") {
            exception = {
                message: exception,
                stack: [],
                name: null,
                cause: null,
                context: null
            }
        }

        let payload;
        if (exception === null) {
            payload = {} 
        } else {
            payload = {
                "exception": exception
            }
        }
        
        this.writeMessage("closed", payload)
        this.closed = true;
        this.messageWriter.close()
    }

    public [Symbol.dispose]() {
        this.close()
    }

    // returns paramAssetKey if given. if not given - asserts that contextAssetKeys is of length 1,
    // and returns the element inside.
    // Also validates that paramAssetKey (if given), is included in contextAssetKeys (if given).
    private resolveAssetKey(paramAssetKey: string | null) {
        const definedAssetKeys = this.contextData.asset_keys;

        if (definedAssetKeys !== null) {
            if (paramAssetKey) { // Make sure assetKey is one of the asset-keys defined in the context
                if (!definedAssetKeys.includes(paramAssetKey)) {
                    throw new DagsterPipesError(
                        `Invalid asset key. Expected one of ${definedAssetKeys}, got ${paramAssetKey}.`
                    )
                }
                return paramAssetKey
            } else { // Make sure only one asset-key is defined in the context, and use it.
                if (definedAssetKeys.length != 1) {
                    throw new DagsterPipesError("Calling `reportAssetMaterialization` without passing an asset key is undefined. Current step targets multiple assets.")
                }
                return definedAssetKeys[0]
            }
        } else { // If there are no definedAssetKeys in the context, use the parameter assetKey.
            if (!paramAssetKey) {
                throw new DagsterPipesError("Calling `reportAssetMaterialization` witout passing an asset key is undefined. Current step does not target a specific asset.")
            }
        }
        return paramAssetKey
    }

    public reportAssetMaterialization(
        metadata: Record<string, any> | null = null,
        dataVersion: string | null = null,
        assetKey: string | null = null
    ): void {
        assetKey = this.resolveAssetKey(assetKey);

        if (this.materializedAssetsList.includes(assetKey)) {
            throw new DagsterPipesError(`Asset ${assetKey} has already been materializaed - so no additional data can be reported for it.`)
        }

        if (metadata !== null) {
            metadata = normalizeMetadata(metadata);    
        }

        this.writeMessage("report_asset_materialization", {
            metadata: metadata,
            data_version: dataVersion,
            asset_key: assetKey
        });

        this.materializedAssetsList.push(assetKey)
    }

    public reportAssetCheck(
        checkName: string,
        passed: boolean,
        severity: string = "ERROR",
        metadata: Record<string, any> | null = null,
        assetKey: string | null = null
    ): void {    
        assetKey = this.resolveAssetKey(assetKey)

        if (metadata !== null) {
            metadata = normalizeMetadata(metadata)
        }

        this.writeMessage(
            "report_asset_check",
            {
                "asset_key": assetKey,
                "check_name": checkName,
                "passed": passed,
                "metadata": metadata,
                "severity": severity
            }
        )
    }
    
    public log(message: string, level: string) {
        this.writeMessage(
            "log",
            {"message": message, "level": level}
        )
    }

    private writeMessage(method: string, params: Record<string, any> = {}): void {
        if (this.closed) {
            throw new DagsterPipesError("cannot send message after PipesContext is closed")
        }

        const message: PipesMessage = {
            __dagster_pipes_version: PIPES_PROTOCOL_VERSION,
            method: method,
            params: params,
        };
        this.messageWriter.writeMessage(message)
    }
    
    public get assetKey() {
        if (this.contextData.asset_keys === null) {
            throw Error("asset_key is unedfined. Current step does not target an asset.");
        }
        if (this.contextData.asset_keys.length != 1) {
            throw Error("asset_key is undefined. Current step targets multiple assets.")
        }
        return this.contextData.asset_keys[0]
    }

    public get assetKeys() {
        if (this.contextData.asset_keys === null) {
            throw Error("asset_keys is unedfined. Current step does not target an asset.");
        }

        return this.contextData.asset_keys;
    }

    public get provenance(): PipesDataProvenance | null {
        if (this.contextData.provenance_by_asset_key === null) {
            throw Error("provenance is undefined. Current step does not target an asset.");
        }
        if (this.contextData.asset_keys === null || this.contextData.asset_keys.length != 1) {
            throw Error("provenance is undefined. Current step targets multiple assets.");
        }
        return this.contextData.provenance_by_asset_key[this.contextData.asset_keys[0]];
    }

    public get provenanceByAssetKey(): Record<string, PipesDataProvenance | null> {
        if (this.contextData.provenance_by_asset_key === null) {
            throw Error("provenance_by_asset_key is undefined. Current step does not target an asset.");
        }
        return this.contextData.provenance_by_asset_key;
    }

    public get codeVersion(): string | null {
        if (this.contextData.code_version_by_asset_key === null) {
            throw Error("code_version is undefined. Current step does not target an asset.");
        }
        if (this.contextData.asset_keys === null || this.contextData.asset_keys.length != 1) {
            throw Error("code_version is undefined. Current step targets multiple assets or no asset.");
        }
        return this.contextData.code_version_by_asset_key[this.contextData.asset_keys[0]];
    }

    public get codeVersionByAssetKey(): Record<string, string | null> {
        if (this.contextData.code_version_by_asset_key === null) {
            throw Error("code_version_by_asset_key is undefined. Current step does not target an asset.");
        }
        return this.contextData.code_version_by_asset_key;
    }

    public get isPartitionStep(): boolean {
        return this.contextData.partition_key_range !== null;
    }

    public get partitionKey(): string {
        if (this.contextData.partition_key === null) {
            throw Error("partition_key is undefined. Current step does not target a partition.");
        }
        return this.contextData.partition_key;
    }

    public get partitionKeyRange(): PipesPartitionKeyRange {
        if (this.contextData.partition_key_range === null) {
            throw Error("partition_key_range is undefined. Current step does not target a partition.");
        }
        return this.contextData.partition_key_range;
    }

    public get partitionTimeWindow(): PipesTimeWindow | null {
        if (this.contextData.partition_key_range === null) {
            throw Error("partition_time_window is undefined. Current step does not target a partition.");
        }
        return this.contextData.partition_time_window;
    }

    public get runID(): string {
        return this.contextData.run_id;
    }

    public get jobName(): string | null {
        return this.contextData.job_name;
    }

    public get retryNumber(): number {
        return this.contextData.retry_number;
    }

    public getExtra(key: string): any {
        if (!(key in this.contextData.extras)) {
            throw Error(`Extra with key ${key} is not defined.`);
        }
        return this.contextData.extras[key];
    }

    public get extras(): Record<string, any> {
        return this.contextData.extras;
    }

    public get isClosed(): boolean {
        return this.closed;
    }
}