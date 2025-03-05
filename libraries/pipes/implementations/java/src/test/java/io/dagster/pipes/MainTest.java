package io.dagster.pipes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.data.PipesAssetCheckSeverity;
import io.dagster.pipes.loaders.PipesContextLoader;
import io.dagster.pipes.loaders.PipesDefaultContextLoader;
import io.dagster.pipes.loaders.PipesS3ContextLoader;
import io.dagster.pipes.writers.PipesDefaultMessageWriter;
import io.dagster.pipes.writers.PipesMessageWriter;
import io.dagster.pipes.writers.PipesMessageWriterChannel;
import io.dagster.pipes.writers.PipesS3MessageWriter;
import picocli.CommandLine;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({
    "PMD.AvoidThrowingRawExceptionTypes", "PMD.UnusedPrivateField",
    "PMD.RedundantFieldInitializer", "PMD.ImmutableField"})
@CommandLine.Command(name = "main-test", mixinStandardHelpOptions = true)
public class MainTest implements Runnable {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private Map<String, Object> cachedJson = new ConcurrentHashMap<>();

    @CommandLine.Option(
        names = {"--job-name"},
        description = "Provide value of 'jobName' for testing"
    )
    private String jobName;

    @CommandLine.Option(
        names = {"--extras"},
        description = "Provide path to 'extras' JSON for testing"
    )
    private String extras;

    @CommandLine.Option(
        names = {"--custom-payload"},
        description = "Specify custom payload path"
    )
    private String customPayloadPath;

    @CommandLine.Option(
        names = {"--report-asset-check"},
        description = "Specify path to JSON with parameters to test reportAssetCheck"
    )
    private String reportAssetCheckJson;

    @CommandLine.Option(
        names = {"--report-asset-materialization"},
        description = "Specify path to JSON with parameters to test reportAssetMaterialization"
    )
    private String reportAssetMaterializationJson;

    @CommandLine.Option(
        names = {"--message-writer"},
        description = "Specify the type of the message writer: default,s3"
    )
    private String messageWriter;

    @CommandLine.Option(
        names = {"--context-loader"},
        description = "Specify the type of the context loader writer: default,s3"
    )
    private String contextLoaderType;

    @CommandLine.Option(
        names = {"--test-name"},
        description = "Specify the name of the test"
    )
    private String testName;

    @Override
    public void run() {
        PipesTests pipesTests = new PipesTests();
        try {
            final PipesContextLoader loader;
            if (this.contextLoaderType != null && !this.contextLoaderType.isEmpty()) {
                if (this.contextLoaderType.equals("s3")) {
                    S3Client amazonS3Client = S3Client.builder().build();
                    loader = new PipesS3ContextLoader(amazonS3Client);
                } else {
                    loader = new PipesDefaultContextLoader();
                }
                pipesTests.setContextLoader(loader);
            }

            final PipesMessageWriter<? extends PipesMessageWriterChannel> writer;
            if (this.messageWriter != null && !this.messageWriter.isEmpty()) {
                if (this.messageWriter.equals("s3")) {
                    S3Client amazonS3Client = S3Client.builder().build();
                    writer = new PipesS3MessageWriter(amazonS3Client);
                } else {
                    writer = new PipesDefaultMessageWriter();
                }
                pipesTests.setMessageWriter(writer);
            }

            // Setup payload if required
            if (this.customPayloadPath != null && !this.customPayloadPath.isEmpty()) {
                cacheJson(this.customPayloadPath);
                Object payload = loadParamByWrapperKey("payload", Object.class);
                pipesTests.setPayload(payload);
            }

            if (this.testName != null && this.testName.equals("test_error_reporting")) {
                pipesTests.testRunPipesSessionWithException();
                return;
            }

            if (this.testName != null && this.testName.equals("test_message_log")) {
                pipesTests.testLogging();
                return;
            }

            // Setup materialization data if required
            if (this.reportAssetMaterializationJson != null && !this.reportAssetMaterializationJson.isEmpty()) {
                cacheJson(this.reportAssetMaterializationJson);
                String dataVersion = loadParamByWrapperKey("dataVersion", String.class);
                String assetKey = loadParamByWrapperKey("assetKey", String.class);
                pipesTests.setMaterialization(dataVersion, assetKey);
            }

            // Setup check data if required
            if (this.reportAssetCheckJson != null && !this.reportAssetCheckJson.isEmpty()) {
                cacheJson(this.reportAssetCheckJson);
                String checkName = loadParamByWrapperKey("checkName", String.class);
                boolean passed = loadParamByWrapperKey("passed", Boolean.class);
                String assetKey = loadParamByWrapperKey("assetKey", String.class);
		        PipesAssetCheckSeverity severity = PipesAssetCheckSeverity.valueOf(
                    loadParamByWrapperKey("severity", String.class)
                );
                pipesTests.setCheck(checkName, passed, assetKey, severity);
            }

            if (this.extras != null) {
                File jsonFile = new File(this.extras);
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> extrasMap = objectMapper.readValue(
                    jsonFile, new TypeReference<Map<String,Object>>() {}
                );
                pipesTests.setExtras(extrasMap);
            }

            if (this.jobName != null) {
                pipesTests.setJobName(this.jobName);
            }

            pipesTests.fullTest();
        } catch (IOException | DagsterPipesException exception) {
            throw new RuntimeException(exception);
        }

        System.out.println("All tests finished.");
    }

    private void cacheJson(String jsonFilePath) {
        try {
            File jsonFile = new File(jsonFilePath);
            this.cachedJson = this.objectMapper.readValue(jsonFile, Map.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load JSON from file: " + jsonFilePath, e);
        }
    }

    private <T> T loadParamByWrapperKey(String wrapperKey, Class<T> type) {
        Object object = this.cachedJson.get(wrapperKey);
        if (object != null && !type.isInstance(object)) {
            throw new IllegalArgumentException(
                String.format(
                    "Wrong type for %s parameter. Expected: %s, found: %s",
                    wrapperKey, type.getTypeName(), object.getClass().getTypeName()
                )
            );
        } else {
            return (T) object;
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new MainTest()).execute(args);
        System.exit(exitCode);
    }
}
