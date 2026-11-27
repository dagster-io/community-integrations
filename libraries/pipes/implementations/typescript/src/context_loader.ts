import * as fs from 'fs';
import * as path from 'path';
import {PipesContextData} from './types';
import {DagsterPipesError} from './errors';

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
    if ('path' in contextParams) {
      return readContextFile(contextParams['path']);
    } else if ('data' in contextParams) {
      return contextParams['data'];
    } else {
      throw new DagsterPipesError(
        `context params must contain either 'path' or 'data' key - but received ${JSON.stringify(contextParams)}`,
      );
    }
  }
}

/**
 * Read the context file named by the `path` context param.
 *
 * The orchestration-side context injector chooses where the file lives, so any location is allowed,
 * but the path must be a string naming a regular file that holds a JSON object.
 */
function readContextFile(rawPath: unknown): PipesContextData {
  if (typeof rawPath !== 'string' || rawPath === '') {
    throw new DagsterPipesError(
      `context param 'path' must be a non-empty string - but received ${JSON.stringify(rawPath)}`,
    );
  }
  const contextFilePath = path.resolve(rawPath);
  let fd: number;
  try {
    // O_NONBLOCK keeps a named pipe from blocking the open; fstat on the open
    // descriptor checks the same file that is then read.
    fd = fs.openSync(contextFilePath, fs.constants.O_RDONLY | fs.constants.O_NONBLOCK);
  } catch (error) {
    throw new DagsterPipesError(`could not open context file ${contextFilePath}: ${error}`);
  }
  let contents: string;
  try {
    if (!fs.fstatSync(fd).isFile()) {
      throw new DagsterPipesError(`context file ${contextFilePath} is not a regular file`);
    }
    contents = fs.readFileSync(fd, 'utf8');
  } finally {
    fs.closeSync(fd);
  }
  let data: unknown;
  try {
    data = JSON.parse(contents);
  } catch {
    // The parse error quotes the file's text, which is not context data.
    throw new DagsterPipesError(`context file ${contextFilePath} does not contain valid JSON`);
  }
  if (data === null || typeof data !== 'object' || Array.isArray(data)) {
    throw new DagsterPipesError(`context file ${contextFilePath} does not contain a JSON object`);
  }
  return data as PipesContextData;
}
