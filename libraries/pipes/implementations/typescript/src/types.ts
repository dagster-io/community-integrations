export enum AssetCheckSeverity {
  Error = 'ERROR',
  Warn = 'WARN',
}

/**
 * The serializable data passed from the orchestration process to the external process. This
 * gets wrapped in a PipesContext.
 */
export interface PipesContextData {
  asset_keys?: string[];
  code_version_by_asset_key?: {[key: string]: null | string};
  extras: {[key: string]: any} | null;
  job_name?: string;
  partition_key?: string;
  partition_key_range?: PartitionKeyRange;
  partition_time_window?: PartitionTimeWindow;
  provenance_by_asset_key?: {[key: string]: null | ProvenanceByAssetKey} | null;
  retry_number: number;
  run_id: string;
}

export interface PartitionKeyRange {
  end?: string;
  start?: string;
  [property: string]: any;
}

export interface PartitionTimeWindow {
  end?: string;
  start?: string;
  [property: string]: any;
}

export interface ProvenanceByAssetKey {
  code_version?: string;
  input_data_versions?: {[key: string]: string};
  is_user_provided?: boolean;
  [property: string]: any;
}

export enum PipesLogLevel {
  Critical = 'CRITICAL',
  Debug = 'DEBUG',
  Error = 'ERROR',
  Info = 'INFO',
  Warning = 'WARNING',
}

export interface PipesMessage {
  /**
   * The version of the Dagster Pipes protocol
   */
  __dagster_pipes_version: string;
  /**
   * Event type
   */
  method: Method;
  /**
   * Event parameters
   */
  params: {[key: string]: any} | null;
}

/**
 * Event type
 */
export enum Method {
  Closed = 'closed',
  Log = 'log',
  Opened = 'opened',
  ReportAssetCheck = 'report_asset_check',
  ReportAssetMaterialization = 'report_asset_materialization',
  ReportCustomMessage = 'report_custom_message',
}

export interface PipesMetadataValue {
  raw_value?: any[] | boolean | number | number | {[key: string]: any} | null | string;
  type?: Type;
  [property: string]: any;
}

export enum Type {
  Asset = 'asset',
  Bool = 'bool',
  DagsterRun = 'dagster_run',
  Float = 'float',
  Infer = '__infer__',
  Int = 'int',
  JSON = 'json',
  Job = 'job',
  Md = 'md',
  Notebook = 'notebook',
  Null = 'null',
  Path = 'path',
  Text = 'text',
  Timestamp = 'timestamp',
  URL = 'url',
}
