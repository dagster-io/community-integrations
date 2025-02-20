export class DagsterPipesError extends Error {}

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
