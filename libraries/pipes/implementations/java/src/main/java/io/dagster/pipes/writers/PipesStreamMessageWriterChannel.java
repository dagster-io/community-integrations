package io.dagster.pipes.writers;

import java.io.OutputStream;
import java.io.PrintWriter;

public class PipesStreamMessageWriterChannel implements PipesMessageWriterChannel {

    private final PrintWriter writer;

    public PipesStreamMessageWriterChannel(final OutputStream outputStream) {
        this.writer = new PrintWriter(outputStream, true);
    }

    @Override
    public void writeMessage(final PipesMessage message) {
        writer.println(message.toString());
    }

    @Override
    public void close() {

    }
}
