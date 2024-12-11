package pipes.writers;

import pipes.DagsterPipesException;
import pipes.data.PipesConstants;
import pipes.utils.PipesUtils;

import java.io.*;
import java.util.Map;

public class PipesDefaultMessageWriter extends PipesMessageWriter<PipesMessageWriterChannel> {

    private static final String STDIO_KEY = "stdio";
    private static final String BUFFERED_STDIO_KEY = "buffered_stdio";
    private static final String STDERR = "stderr";
    private static final String STDOUT = "stdout";

    @Override
    public PipesMessageWriterChannel open(Map<String, Object> params) throws DagsterPipesException {
        if (params.containsKey(PipesConstants.PATH_KEY.name)) {
            String path = PipesUtils.assertParamType(
                params,
                PipesConstants.PATH_KEY.name,
                String.class,
                PipesDefaultMessageWriter.class
            );
            return new PipesFileMessageWriterChannel(path);
        } else if (params.containsKey(STDIO_KEY)) {
            String stream = PipesUtils.assertParamType(
                params,
                STDIO_KEY,
                String.class,
                PipesDefaultMessageWriter.class
            );
            OutputStream target = getTarget(stream, STDIO_KEY);
            return new PipesStreamMessageWriterChannel(target);
        } else if (params.containsKey(BUFFERED_STDIO_KEY)) {
            String stream = PipesUtils.assertParamType(
                params,
                BUFFERED_STDIO_KEY,
                String.class,
                PipesDefaultMessageWriter.class
            );
            PipesBufferedStreamMessageWriterChannel channel;
            try (OutputStream target = getTarget(BUFFERED_STDIO_KEY, stream)) {
                channel = new PipesBufferedStreamMessageWriterChannel(target);
                channel.flush();
            } catch (IOException ioe) {
                throw new DagsterPipesException(
                    String.format("Failed to flush with %s", BUFFERED_STDIO_KEY), ioe
                );
            }
            return channel;
        } else {
            throw new DagsterPipesException(String.format(
                "Invalid params for %s, expected key \"%s\" or \"%s\", received %s",
                this.getClass().getSimpleName(), PipesConstants.PATH_KEY.name, STDIO_KEY, params)
            );
        }
    }

    private static OutputStream getTarget(String stream, String ioKey) throws DagsterPipesException {
        if (!STDERR.equals(stream) && !STDOUT.equals(stream)) {
            throw new DagsterPipesException(String.format(
                "Invalid value for key \"%s\", expected \"%s\" or \"%s\" but received \"%s\"",
                ioKey, STDERR, STDOUT, stream
            ));
        }
        return STDERR.equals(stream) ? System.err : System.out;
    }

}
