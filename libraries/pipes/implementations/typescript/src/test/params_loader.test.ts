import { PipesEnvParamsLoader } from "../params_loader";
import * as zlib from "zlib";

describe('PipesEnvParamsLoader', () => {
    let originalEnv: NodeJS.ProcessEnv;

    beforeEach(() => {
        originalEnv = process.env;
        process.env = { ...originalEnv };
    });

    afterEach(() => {
        process.env = originalEnv;
    });

    test('loads context params from environment variable', () => {
        const contextData = { key: "value" };
        const encodedContextData = zlib.deflateSync(Buffer.from(JSON.stringify(contextData))).toString("base64");
        process.env.DAGSTER_PIPES_CONTEXT = encodedContextData;

        const paramsLoader = new PipesEnvParamsLoader();
        const result = paramsLoader.loadContextParams();
        expect(result).toEqual(contextData);
    });

    test('loads message params from environment variable', () => {
        const messageData = { key: "value" };
        const encodedMessageData = zlib.deflateSync(Buffer.from(JSON.stringify(messageData))).toString("base64");
        process.env.DAGSTER_PIPES_MESSAGES = encodedMessageData;

        const paramsLoader = new PipesEnvParamsLoader();
        const result = paramsLoader.loadMessagesParams();
        expect(result).toEqual(messageData);
    });

    test('returns true for isDagsterPipesProcess when context env var is set', () => {
        process.env.DAGSTER_PIPES_CONTEXT = "some_value";

        const paramsLoader = new PipesEnvParamsLoader();
        const result = paramsLoader.isDagsterPipesProcess();
        expect(result).toBe(true);
    });

    test('returns false for isDagsterPipesProcess when context env var is not set', () => {
        delete process.env.DAGSTER_PIPES_CONTEXT;

        const paramsLoader = new PipesEnvParamsLoader();
        const result = paramsLoader.isDagsterPipesProcess();
        expect(result).toBe(false);
    });
});
