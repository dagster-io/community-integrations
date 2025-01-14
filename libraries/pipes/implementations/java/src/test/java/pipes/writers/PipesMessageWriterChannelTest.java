package pipes.writers;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pipes.DagsterPipesException;
import pipes.PipesTests;
import types.Method;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PipesMessageWriterChannelTest {

    @TempDir
    private static Path tempDir;

    private static PipesMessage message;

    private static final String VALID_JSON = "{\"__dagster_pipes_version\":\"0.1\",\"method\":\"report_asset_materialization\",\"params\":{\"bool_true\":{\"raw_value\":true,\"type\":\"bool\"},\"float\":{\"raw_value\":0.1,\"type\":\"float\"},\"int\":{\"raw_value\":1,\"type\":\"int\"},\"url\":{\"raw_value\":\"https://dagster.io\",\"type\":\"url\"},\"path\":{\"raw_value\":\"/dev/null\",\"type\":\"path\"},\"null\":{\"raw_value\":null,\"type\":\"null\"},\"md\":{\"raw_value\":\"**markdown**\",\"type\":\"md\"},\"json\":{\"raw_value\":{\"quux\":{\"a\":1,\"b\":2},\"corge\":null,\"qux\":[1,2,3],\"foo\":\"bar\",\"baz\":1},\"type\":\"json\"},\"bool_false\":{\"raw_value\":false,\"type\":\"bool\"},\"text\":{\"raw_value\":\"hello\",\"type\":\"text\"},\"asset\":{\"raw_value\":\"foo/bar\",\"type\":\"asset\"},\"dagster_run\":{\"raw_value\":\"db892d7f-0031-4747-973d-22e8b9095d9d\",\"type\":\"dagster_run\"},\"notebook\":{\"raw_value\":\"notebook.ipynb\",\"type\":\"notebook\"}}}";

    @BeforeAll
    public static void formPipesMessage() {
        Map<String, ?> metadata = new PipesTests().buildTestMetadata();
        message = new PipesMessage(
            "0.1", Method.REPORT_ASSET_MATERIALIZATION.toValue(), metadata
        );
    }

    @Test
    void testPipesFileMessageWriterChannel() throws DagsterPipesException, IOException {
        Path filePath = tempDir.resolve("message.txt");
        PipesFileMessageWriterChannel fileWriter = new PipesFileMessageWriterChannel(
            filePath.toString()
        );
        fileWriter.writeMessage(message);
        String content = Files.readAllLines(filePath).get(0);
        assertEquals(VALID_JSON, content);
    }

    @Test
    void testPipesStreamMessageWriterChannel() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PipesStreamMessageWriterChannel channel = new PipesStreamMessageWriterChannel(outputStream);
        channel.writeMessage(message);
        String result = outputStream.toString();
        assertEquals(VALID_JSON + System.lineSeparator(), result);
    }

    @Test
    void testPipesBufferedStreamMessageWriterChannel() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PipesBufferedStreamMessageWriterChannel channel =
            new PipesBufferedStreamMessageWriterChannel(outputStream);
        channel.writeMessage(message);
        channel.flush();
        String result = outputStream.toString();
        assertEquals(VALID_JSON + System.lineSeparator(), result);
    }
}
