package io.dagster.pipes.logger;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.utils.PipesUtils;
import io.dagster.pipes.writers.PipesMessageWriterChannel;
import io.dagster.types.Method;
import io.dagster.types.PipesLog;

public class PipesLogger {

    private final Logger logger;

    private final PipesMessageWriterChannel writerChannel;

    public PipesLogger(Logger logger, PipesMessageWriterChannel writerChannel) {
        this.logger = logger;
        this.writerChannel = writerChannel;
    }

    public void debug(String message) throws DagsterPipesException {
        log(new PipesLogLevel(PipesLog.DEBUG.toValue(), 500), message);
    }

    public void info(String message) throws DagsterPipesException {
        log(new PipesLogLevel(PipesLog.INFO.toValue(), 800), message);
    }

    public void warning(String message) throws DagsterPipesException {
        log(new PipesLogLevel(PipesLog.WARNING.toValue(), 900), message);
    }

    public void error(String message) throws DagsterPipesException {
        log(new PipesLogLevel(PipesLog.ERROR.toValue(), 1000), message);
    }

    public void critical(String message) throws DagsterPipesException {
        log(new PipesLogLevel(PipesLog.CRITICAL.toValue(), 1100), message);
    }

    private void log(PipesLogLevel level, String message) throws DagsterPipesException {
        Map<String, String> logRecordParams = new HashMap<>();
        logRecordParams.put("message", message);
        logRecordParams.put("level", level.getName());
        this.writerChannel.writeMessage(PipesUtils.makeMessage(Method.LOG, logRecordParams));
        this.logger.log(new LogRecord(level, message));
    }
}
