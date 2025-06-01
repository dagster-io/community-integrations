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
    String pipesContextParams;

    @Option(
        names = "--dagster-pipes-messages", 
        description = "base64 encoded and zlib-compressed messages params",
        defaultValue = "")
    String pipesMessagesParams;
    
    public PipesCliArgsParamsLoader(final String... args) {
        final CommandLine commandLine = new CommandLine(this);
        commandLine.setUnmatchedArgumentsAllowed(true);
        commandLine.setUnmatchedOptionsArePositionalParams(true);

        commandLine.parseArgs(args);
    }

    @Override
    public boolean isDagsterPipesProcess() {
        return !"".equals(pipesContextParams);
    }

    @Override
    public Optional<Map<String, Object>> loadContextParams() throws DagsterPipesException {
        return Optional.of(PipesUtils.decodeParam(pipesContextParams));
    }

    @Override
    public Optional<Map<String, Object>> loadMessagesParams() throws DagsterPipesException {
        return Optional.of(PipesUtils.decodeParam(pipesMessagesParams));
    }
}
