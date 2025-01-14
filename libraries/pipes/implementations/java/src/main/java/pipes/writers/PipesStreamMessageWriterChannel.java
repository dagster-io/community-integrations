package pipes.writers;

import java.io.OutputStream;
import java.io.PrintWriter;

public class PipesStreamMessageWriterChannel implements PipesMessageWriterChannel {

    private final PrintWriter writer;

    public PipesStreamMessageWriterChannel(OutputStream outputStream) {
        this.writer = new PrintWriter(outputStream, true);
    }

    @Override
    public void writeMessage(PipesMessage message) {
        writer.println(message.toString());
    }

    @Override
    public void close() {

    }
}
