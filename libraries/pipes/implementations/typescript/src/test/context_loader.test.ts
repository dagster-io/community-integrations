import {PipesDefaultContextLoader} from '../context_loader';
import {DagsterPipesError} from '../errors';
import {PipesContextData} from '../types';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {execFileSync} from 'child_process';

const contextData: PipesContextData = {
  asset_keys: ['asset1'],
  code_version_by_asset_key: {asset1: 'v1'},
  provenance_by_asset_key: {asset1: null},
  run_id: 'run123',
  job_name: 'job1',
  retry_number: 0,
  extras: {},
};

test('loads context from file', () => {
  const contextLoader = new PipesDefaultContextLoader();

  const tmpfile: string = path.join(os.tmpdir(), 'tmp_file');

  fs.writeFileSync(tmpfile, JSON.stringify(contextData));

  try {
    const contextParams = {path: tmpfile};

    const result = contextLoader.loadContext(contextParams);
    expect(result).toEqual(contextData);
  } finally {
    fs.rmSync(tmpfile);
  }
});

test('loads context from data', () => {
  const contextLoader = new PipesDefaultContextLoader();

  const contextParams = {data: contextData};
  const result = contextLoader.loadContext(contextParams);
  expect(result).toEqual(contextData);
});

test('throws error if neither path nor data is provided', () => {
  const contextLoader = new PipesDefaultContextLoader();
  const contextParams = {invalid: 'param'};
  expect(() => contextLoader.loadContext(contextParams)).toThrow(DagsterPipesError);
});

describe('context file path validation', () => {
  let tmpdir: string;

  beforeEach(() => {
    tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipes-context-'));
  });

  afterEach(() => {
    fs.rmSync(tmpdir, {recursive: true, force: true});
  });

  const writeContextFile = (name: string, contents: string): string => {
    const file = path.join(tmpdir, name);
    fs.writeFileSync(file, contents);
    return file;
  };

  test.each([[42], [null], [{}], [['/tmp/context']], ['']])(
    'rejects a path that is not a non-empty string: %p',
    (badPath) => {
      const contextLoader = new PipesDefaultContextLoader();
      expect(() => contextLoader.loadContext({path: badPath})).toThrow(DagsterPipesError);
    },
  );

  test.each([
    ['a directory', () => tmpdir],
    ['a missing file', () => path.join(tmpdir, 'missing')],
    ['a character device', () => '/dev/null'],
    [
      'a named pipe',
      () => {
        const fifo = path.join(tmpdir, 'fifo');
        execFileSync('mkfifo', [fifo]);
        return fifo;
      },
    ],
  ])('rejects %s', (_description, makePath) => {
    const contextLoader = new PipesDefaultContextLoader();
    expect(() => contextLoader.loadContext({path: makePath()})).toThrow(DagsterPipesError);
  });

  test.each([['[1, 2]'], ['"a string"'], ['null']])(
    'rejects a context file whose JSON is not an object: %s',
    (contents) => {
      const contextLoader = new PipesDefaultContextLoader();
      const file = writeContextFile('context', contents);
      expect(() => contextLoader.loadContext({path: file})).toThrow(DagsterPipesError);
    },
  );

  test('does not echo the contents of a file that is not JSON', () => {
    const contextLoader = new PipesDefaultContextLoader();
    const file = writeContextFile('secrets', 'root:x:0:0:root:/root:/bin/bash');
    const load = () => contextLoader.loadContext({path: file});
    expect(load).toThrow(DagsterPipesError);
    expect(load).not.toThrow(/root:x/);
  });

  test('loads a context file given a relative path', () => {
    const contextLoader = new PipesDefaultContextLoader();
    const file = writeContextFile('context', JSON.stringify(contextData));
    const relative = path.relative(process.cwd(), file);
    expect(contextLoader.loadContext({path: relative})).toEqual(contextData);
  });
});
