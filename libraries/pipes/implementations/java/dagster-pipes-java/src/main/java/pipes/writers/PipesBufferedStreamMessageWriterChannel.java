package pipes.writers;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class PipesBufferedStreamMessageWriterChannel implements PipesMessageWriterChannel {

    private final List<PipesMessage> buffer;
    private final BufferedWriter stream;

    public PipesBufferedStreamMessageWriterChannel(OutputStream outputStream) {
        this.buffer = new ArrayList<>();
        this.stream = new BufferedWriter(new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeMessage(PipesMessage message) {
        buffer.add(message);
    }

    public void flush() throws IOException {
        try {
            // Iterate through buffered messages and write each one to the stream
            for (PipesMessage message : buffer) {
                stream.write(message.toString());
                stream.newLine();
            }
            buffer.clear();
        } finally {
            stream.flush();
        }
    }

    @Override
    public void close() {}
}
