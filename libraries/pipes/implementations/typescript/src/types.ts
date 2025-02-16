export class DagsterPipesError extends Error {}

/**
 * A message sent from the external process to the orchestration process.
 */
export interface PipesMessage {
    __dagster_pipes_version: string;
    method: string;
    params: Record<string, any>;
}

/**
 * A range of partition keys.
 */
export interface PipesPartitionKeyRange {
    start: string;
    end: string;
}

/**
 * A span of time delimited by a start and end timestamp. This is defined for time-based partitioning schemes.
 */
export interface PipesTimeWindow {
    start: string; // timestamp
    end: string; // timestamp
}

/**
 * Provenance information for an asset.
 */
export interface PipesDataProvenance {
    code_version: string;
    input_data_versions: Record<string, string>;
    is_user_provided: boolean;
}

/**
 * The serializable data passed from the orchestration process to the external process. This gets
 * wrapped in a `PipesContext`.
 */
export interface PipesContextData {
    asset_keys: string[] | null;
    code_version_by_asset_key: Record<string, string | null> | null;
    provenance_by_asset_key: Record<string, PipesDataProvenance | null> | null;
    partition_key: string | null;
    partition_key_range: PipesPartitionKeyRange | null;
    partition_time_window: PipesTimeWindow | null;
    run_id: string;
    job_name: string | null;
    retry_number: number;
    extras: Record<string, any>;
}

/**
 * An exception that can be reported from the external process to the Dagster orchestration process.
 */
export interface PipesException {
    message: string;
    stack: string[];
    name: string | null;
    cause: PipesException | null;
    context: PipesException | null;
}