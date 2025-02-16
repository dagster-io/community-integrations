import { PipesDefaultMessageWriter } from "../message_writer";
import { DagsterPipesError, PipesMessage } from "../types";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";

const message1: PipesMessage = {
    __dagster_pipes_version: "0.1",
    method: "test_method",
    params: { key: "value" }
};

const message2: PipesMessage = {
    __dagster_pipes_version: "0.1",
    method: "test_method",
    params: { key2: "value2" }
};

describe('PipesDefaultMessageWriter', () => {
    test('writes message to file', () => {
        const messageWriter = new PipesDefaultMessageWriter()

        const tmpfile: string = path.join(os.tmpdir(), "messages.log");

        try {
            messageWriter.open({ path: tmpfile });
            messageWriter.writeMessage(message1);
            messageWriter.writeMessage(message2);
            messageWriter.close()

            const writtenMessage = fs.readFileSync(tmpfile, "utf8");
            expect(writtenMessage).toBe(
                JSON.stringify(message1) + "\n" + 
                JSON.stringify(message2) + "\n");
        } finally {
            fs.rmSync(tmpfile);
        }
    });

    test('writes message to stdout', () => {
        const messageWriter = new PipesDefaultMessageWriter()

        console.log = jest.fn();
        const messagesParams = { stdio: "stdout" };
        messageWriter.open(messagesParams);
        messageWriter.writeMessage(message1);
        messageWriter.writeMessage(message2);
        messageWriter.close()

        expect(console.log).toHaveBeenCalledWith(JSON.stringify(message1));
        expect(console.log).toHaveBeenCalledWith(JSON.stringify(message2));
    });

    test('writes message to stderr', () => {
        const messageWriter = new PipesDefaultMessageWriter()

        console.error = jest.fn();
        const messagesParams = { stdio: "stderr" };
        
        messageWriter.open(messagesParams);
        messageWriter.writeMessage(message1);
        messageWriter.writeMessage(message2);
        messageWriter.close()

        expect(console.error).toHaveBeenCalledWith(JSON.stringify(message1));
        expect(console.log).toHaveBeenCalledWith(JSON.stringify(message2));
    });

    test('buffers messages and writes to stdout on close', () => {
        const messageWriter = new PipesDefaultMessageWriter()

        console.log = jest.fn();
        const messagesParams = { buffered_stdio: "stdout" };
        
        messageWriter.open(messagesParams);
        messageWriter.writeMessage(message1);
        messageWriter.writeMessage(message2);
        expect(console.log).not.toHaveBeenCalled()
        messageWriter.close();
        
        expect(console.log).toHaveBeenCalledWith(JSON.stringify(message1));
        expect(console.log).toHaveBeenCalledWith(JSON.stringify(message2));
    });

    test('buffers messages and writes to stderr on close', () => {
        const messageWriter = new PipesDefaultMessageWriter()

        console.error = jest.fn();
        const messagesParams = { buffered_stdio: "stderr" };
        
        messageWriter.open(messagesParams);
        messageWriter.writeMessage(message1);
        messageWriter.writeMessage(message2);
        expect(console.error).not.toHaveBeenCalled()
        messageWriter.close();
        
        expect(console.error).toHaveBeenCalledWith(JSON.stringify(message1));
        expect(console.error).toHaveBeenCalledWith(JSON.stringify(message2));
    });

    test('throws error if writeMessage is called before open', () => {
        const messageWriter = new PipesDefaultMessageWriter()

        expect(() => messageWriter.writeMessage(message1)).toThrow(DagsterPipesError);
    });

    test('throws error if writeMessage is called after close', () => {
        const messageWriter = new PipesDefaultMessageWriter()

        const messagesParams = { stdio: "stdout" };
        messageWriter.open(messagesParams);
        messageWriter.close();
        expect(() => messageWriter.writeMessage(message1)).toThrow(DagsterPipesError);
    });
});


