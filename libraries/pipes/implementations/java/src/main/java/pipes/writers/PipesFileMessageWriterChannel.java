package pipes.writers;

import pipes.DagsterPipesException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class PipesFileMessageWriterChannel implements PipesMessageWriterChannel {

    private final String path;

    public PipesFileMessageWriterChannel(String path) {
        this.path = path;
    }

    @Override
    public void writeMessage(PipesMessage message) throws DagsterPipesException {
        File file = new File(this.path);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists() && !parentDir.mkdirs()) {
            throw new DagsterPipesException("Failed to create directories for file: " + path);
        }

        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(file.toPath(),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        ) {
            System.out.println(file.getAbsolutePath());
            bufferedWriter.write(message.toString());
            bufferedWriter.newLine();
        } catch (IOException ioException) {
            throw new DagsterPipesException("Failed to write to the file: " + file.getAbsolutePath(), ioException);
        }
    }

    public String getPath() {
        return path;
    }

    @Override
    public void close() {}
}
