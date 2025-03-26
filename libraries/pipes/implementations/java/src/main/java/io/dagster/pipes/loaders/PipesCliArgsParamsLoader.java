package io.dagster.pipes.loaders;

import java.util.Map;
import java.util.Optional;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.utils.PipesUtils;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class PipesCliArgsParamsLoader implements PipesParamsLoader {
    @Option(
        names = "--dagster-pipes-context", 
        description = "base64 encoded and zlib-compressed context loader params",
        defaultValue = "")
    String pipes_context_params;

    @Option(
        names = "--dagster-pipes-messages", 
        description = "base64 encoded and zlib-compressed messages params",
        defaultValue = "")
    String pipes_messages_params;
    
    public PipesCliArgsParamsLoader(String[] args) {
        CommandLine commandLine = new CommandLine(this);
        commandLine.setUnmatchedArgumentsAllowed(true);
        commandLine.setUnmatchedOptionsArePositionalParams(true);

        commandLine.parseArgs(args);
    }

    public boolean isDagsterPipesProcess() {
        return !pipes_context_params.equals("");
    }

    @Override
    public Optional<Map<String, Object>> loadContextParams() throws DagsterPipesException {
        return Optional.of(PipesUtils.decodeParam(pipes_context_params));
    }

    @Override
    public Optional<Map<String, Object>> loadMessagesParams() throws DagsterPipesException {
        return Optional.of(PipesUtils.decodeParam(pipes_messages_params));
    }
}
