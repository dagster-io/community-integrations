package io.dagster.pipes.writers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class PipesBufferedStreamMessageWriterChannel implements PipesMessageWriterChannel {

    private final List<PipesMessage> buffer;
    private final BufferedWriter stream;

    public PipesBufferedStreamMessageWriterChannel(final OutputStream outputStream) {
        this.buffer = new ArrayList<>();
        this.stream = new BufferedWriter(new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeMessage(final PipesMessage message) {
        buffer.add(message);
    }

    /**
     * Writes all buffered messages to the underlying output stream and flushes it.
     *
     * <p>The underlying stream is guaranteed to be flushed even if an exception occurs during writing.
     *
     * @throws IOException if any I/O error occurs while writing to the stream or during final flushing
     */
    public void flush() throws IOException {
        try {
            // Iterate through buffered messages and write each one to the stream
            for (final PipesMessage message : this.buffer) {
                this.stream.write(message.toString());
                this.stream.newLine();
            }
            this.buffer.clear();
        } finally {
            this.stream.flush();
        }
    }

    @Override
    public void close() {

    }
}
