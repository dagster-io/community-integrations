import { PipesDefaultContextLoader } from "../context_loader";
import { DagsterPipesError, PipesContextData } from "../types";
import * as fs from "fs";
import * as path from "path"
import * as os from 'os';

const contextData: PipesContextData = {
    asset_keys: ["asset1"],
    code_version_by_asset_key: { "asset1": "v1" },
    provenance_by_asset_key: { "asset1": null },
    partition_key: null,
    partition_key_range: null,
    partition_time_window: null,
    run_id: "run123",
    job_name: "job1",
    retry_number: 0,
    extras: {}
};

test('loads context from file', () => {
    const contextLoader = new PipesDefaultContextLoader();

    const tmpfile: string = path.join(os.tmpdir(), "tmp_file")

    fs.writeFileSync(tmpfile, JSON.stringify(contextData))

    try {
        const contextParams = { path: tmpfile };

        const result = contextLoader.loadContext(contextParams);
        expect(result).toEqual(contextData);
    }
    finally {
        fs.rmSync(tmpfile)
    }    
});

test('loads context from data', () => {
    const contextLoader = new PipesDefaultContextLoader();

    const contextParams = { data: contextData };
    const result = contextLoader.loadContext(contextParams);
    expect(result).toEqual(contextData);
});

test('throws error if neither path nor data is provided', () => {
    const contextLoader = new PipesDefaultContextLoader();
    const contextParams = { invalid: "param" };
    expect(() => contextLoader.loadContext(contextParams)).toThrow(DagsterPipesError);
});