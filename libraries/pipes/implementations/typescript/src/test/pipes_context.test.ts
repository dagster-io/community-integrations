import { openDagsterPipes, PIPES_PROTOCOL_VERSION, PipesContext } from "../pipes_context";

const contextData = {
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

describe('openDagsterPipes', () => {
    
    afterEach(() => {
        PipesContext["instance"] = null; // Reset the singleton after each test
    })

    test('pipes context data is the same as injected context data', () => {
        const paramsLoader = {
            loadContextParams: jest.fn().mockReturnValue({"context_param": "param"}),
            loadMessagesParams: jest.fn().mockReturnValue({"messages_param": "param"}),
            isDagsterPipesProcess: jest.fn().mockReturnValue(true)
        };
        const contextLoader = {
            loadContext: jest.fn().mockReturnValue(contextData)
        };
        const messagesWriter = {
            open: jest.fn(),
            writeMessage: jest.fn(),
            close: jest.fn(),
            openedExtras: jest.fn()
        };
        
        const pipes = openDagsterPipes(paramsLoader, contextLoader, messagesWriter);

        expect(paramsLoader.loadContextParams).toHaveBeenCalled();
        expect(contextLoader.loadContext).toHaveBeenCalledWith({"context_param": "param"})

        expect(pipes.assetKey).toEqual("asset1");
        expect(pipes.assetKeys).toEqual(["asset1"]);
        expect(pipes.runID).toEqual("run123");
        expect(pipes.jobName).toEqual("job1");
        expect(pipes.retryNumber).toEqual(0);
        expect(pipes.provenanceByAssetKey).toEqual({ "asset1": null });
        expect(pipes.codeVersionByAssetKey).toEqual({ "asset1": "v1" });
        expect(pipes.extras).toEqual({});
        expect(pipes.isPartitionStep).toBe(false);

        pipes.close();
        expect(pipes.isClosed).toBe(true);
    });

    test('test message flow is correct (opened, log, reportAsset, reportCheck, closed)', () => {
        const paramsLoader = {
            loadContextParams: jest.fn().mockReturnValue({"context_param": "param"}),
            loadMessagesParams: jest.fn().mockReturnValue({"messages_param": "param"}),
            isDagsterPipesProcess: jest.fn().mockReturnValue(true)
        };
        const contextLoader = {
            loadContext: jest.fn().mockReturnValue(contextData)
        };
        const messagesWriter = {
            open: jest.fn(),
            writeMessage: jest.fn(),
            close: jest.fn(),
            openedExtras: jest.fn().mockReturnValue(
                {"extra_key": "extra_value"}
            )
        };
        
        const pipes = openDagsterPipes(paramsLoader, contextLoader, messagesWriter);

        expect(paramsLoader.loadContextParams).toHaveBeenCalled();
        expect(contextLoader.loadContext).toHaveBeenCalledWith({"context_param": "param"})
        expect(messagesWriter.open).toHaveBeenCalledWith({"messages_param": "param"})

        expect(messagesWriter.writeMessage).toHaveBeenCalledWith({
            __dagster_pipes_version: PIPES_PROTOCOL_VERSION,
            method: "opened",
            params: {"extras": {"extra_key": "extra_value"}}
        })

        pipes.log("this is an info", "INFO")
        expect(messagesWriter.writeMessage).toHaveBeenCalledWith(expect.objectContaining({
            method: "log",
            params: expect.objectContaining({
                message: "this is an info",
                level: "INFO"
            })
        }));

        pipes.logger.warning("this is a warning")
        expect(messagesWriter.writeMessage).toHaveBeenCalledWith(expect.objectContaining({
            method: "log",
            params: expect.objectContaining({
                message: "this is a warning",
                level: "WARNING"
            })
        }));

        pipes.reportAssetMaterialization({ key: "value" }, "v1");
        expect(messagesWriter.writeMessage).toHaveBeenCalledWith(expect.objectContaining({
            method: "report_asset_materialization",
            params: expect.objectContaining({
                metadata: { key: {raw_value: "value", type: "__infer__"} }, // expect metadata to be normalized
                data_version: "v1",
                asset_key: "asset1"
            })
        }));

        pipes.reportAssetCheck("check1", true, "ERROR", { key: "value" });
        expect(messagesWriter.writeMessage).toHaveBeenCalledWith(expect.objectContaining({
            method: "report_asset_check",
            params: expect.objectContaining({
                asset_key: "asset1",
                check_name: "check1",
                passed: true,
                metadata: {key: {raw_value: "value", type: "__infer__"}}, // expect metadata to be normalized
                severity: "ERROR"
            })
        }));

        pipes.close();
        expect(messagesWriter.writeMessage).toHaveBeenCalledWith({
            __dagster_pipes_version: PIPES_PROTOCOL_VERSION,
            method: "closed",
            params: {}
        });
        expect(messagesWriter.close).toHaveBeenCalled();
    });

    test('opening from outside dagster context is a no-op', () => {
        const paramsLoader = {
            loadContextParams: jest.fn().mockReturnValue({}),
            loadMessagesParams: jest.fn().mockReturnValue({}),
            isDagsterPipesProcess: jest.fn().mockReturnValue(false)
        };
        const contextLoader = {
            loadContext: jest.fn()
        };
        const messagesWriter = {
            open: jest.fn(),
            writeMessage: jest.fn(),
            close: jest.fn(),
            openedExtras: jest.fn()
        };

        const pipes = openDagsterPipes(paramsLoader, contextLoader, messagesWriter);

        expect(paramsLoader.loadContextParams).not.toHaveBeenCalled();
        expect(contextLoader.loadContext).not.toHaveBeenCalled();
        expect(messagesWriter.open).not.toHaveBeenCalled();
        expect(messagesWriter.writeMessage).not.toHaveBeenCalled();

        pipes.log("this is an info", "INFO");
        expect(messagesWriter.writeMessage).not.toHaveBeenCalled();

        pipes.reportAssetMaterialization({ key: "value" }, "v1");
        expect(messagesWriter.writeMessage).not.toHaveBeenCalled();

        pipes.reportAssetCheck("check1", true, "ERROR", { key: "value" });
        expect(messagesWriter.writeMessage).not.toHaveBeenCalled();

        pipes.close();
        expect(messagesWriter.writeMessage).not.toHaveBeenCalled();
        expect(messagesWriter.close).not.toHaveBeenCalled();
    });
});
