import { DagsterPipesError } from "./types"
import * as fs from "fs"
import { PipesMessage } from "./types"

/**
 * An abstract class that defines the way Dagster Pipes messages should be reported back to the 
 * orchestration process.
 */
export abstract class PipesMessageWriter {
    public constructor() {}

    /**
     * Opens the message writer. will be called by `openDagsterPipes()`.
     *
     * @param messagesParams - Parameters for opening the messageWriter (from PipesParamsLoader).
     */
    public abstract open(messagesParams: Record<string, any>): void;

    /**
     * Write a message back to the orchestration process.
     *
     * @param message - The message to write.
     */
    public abstract writeMessage(message: PipesMessage): void;

    /**
     * Close the message writer - will be called by `PipesContest.close()`
     */
    public abstract close(): void;

    /**
     * Override this method in order to add extras that will be reported
     * to the orchestration process, together with the `opened` message.
     * By default - an empty dictionary.
     * 
     * @returns A record of opened extras.
     */
    public openedExtras(): Record<string, any> {
        return {}
    }
}

enum WriteDestination {
    NONE,
    FILE,
    STDOUT,
    STDERR,
}

const STDIO = "stdio"
const BUFFERED_STDIO = "buffered_stdio"
const STDOUT = "stdout"
const STDERR = "stderr"
const INCLUDE_STDIO_IN_MESSAGES_KEY = "include_stdio_in_messages"

/** 
 * The default message writer supports writing to either a file (specified in `path` key),
 * or to stdout/stderr (specified in the `stdio`/`buffered_stdio` key
 */
export class PipesDefaultMessageWriter extends PipesMessageWriter {
    private filePath: string = ""
    private writeDestination: WriteDestination = WriteDestination.NONE
    private shouldBuffer: boolean = false
    private buffer: string[] = []
    private isOpen: boolean = false;
    private isClosed: boolean = false

    public open(messagesParams: Record<string, any>): void {
        this.isOpen = true;
        if (messagesParams["path"]) {
            this.filePath = messagesParams["path"]
            this.writeDestination = WriteDestination.FILE;
            // TODO: support capturing the external process's stdout and stderr 
            if (messagesParams[INCLUDE_STDIO_IN_MESSAGES_KEY]) { 
                throw new DagsterPipesError("include_stdio_in_messages is not yet implemented in the typescript-pipes framework")
            }
        } else if (messagesParams[STDIO]) {
            if (messagesParams[STDIO] == STDOUT) {
                this.writeDestination = WriteDestination.STDOUT;
            } else if (messagesParams[STDIO] == STDERR) {
                this.writeDestination = WriteDestination.STDERR;
            } else {
                throw new DagsterPipesError(`valud for 'stdio' key must be either 'stdout' or 'stderr'. Receeived ${messagesParams["stdio"]}`)
            }
        } else if (messagesParams[BUFFERED_STDIO]) {
            this.shouldBuffer = true;
            if (messagesParams[BUFFERED_STDIO] == STDOUT) {
                this.writeDestination = WriteDestination.STDOUT;
            } else if (messagesParams[BUFFERED_STDIO] == STDERR) {
                this.writeDestination = WriteDestination.STDERR;
            } else {
                throw new DagsterPipesError(`value for 'buffered_stdio' key must be either 'stdout' or 'stderr'. Received ${messagesParams["buffered_stdio"]}`);
            }
        } else {
            throw new DagsterPipesError("Either 'path', 'stdio', or 'buffered_stdio' must be provided in messagesParams")
        }
    }

    public writeMessage(message: PipesMessage): void {
        if (!this.isOpen) {
            throw new DagsterPipesError("MessageWriter.writeMessage cannot be called before MessageWriter.open()")
        }
        if (this.isClosed) {
            throw new DagsterPipesError("MessageWriter.writeMessage cannot be called after MessageWriter.close()")
        }
        
        const messageJson = JSON.stringify(message)

        if (this.shouldBuffer) {
            this.buffer.push(messageJson)
            return;
        }

        switch (this.writeDestination) {
            case WriteDestination.FILE: {
                fs.appendFileSync(this.filePath, messageJson + "\n")
                break;
            }
            case WriteDestination.STDOUT: {
                console.log(messageJson)
                break;
            }
            case WriteDestination.STDERR: {
                console.error(messageJson)
                break;
            }
        }
    }

    public close(): void {
        if (this.isClosed) {
            return;
        }
        this.isClosed = true;

        if (!this.shouldBuffer) {
            return;
        }

        for(const line of this.buffer) {
            switch (this.writeDestination) {
                case WriteDestination.STDOUT: {
                    console.log(line)
                    break;
                }
                case WriteDestination.STDERR: {
                    console.error(line)
                    break;
                }
            }
        }
    }
}