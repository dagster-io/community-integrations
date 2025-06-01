package io.dagster.pipes.writers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import io.dagster.pipes.DagsterPipesException;

public class PipesFileMessageWriterChannel implements PipesMessageWriterChannel {

    private final String path;

    public PipesFileMessageWriterChannel(final String path) {
        this.path = path;
    }

    @Override
    public void writeMessage(final PipesMessage message) throws DagsterPipesException {
        final File file = new File(this.path);
        final File parentDir = file.getParentFile();
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
