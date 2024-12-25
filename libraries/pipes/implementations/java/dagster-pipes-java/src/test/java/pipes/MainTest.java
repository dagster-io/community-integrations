package pipes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;
import pipes.data.PipesAssetCheckSeverity;
import pipes.data.PipesConstants;
import pipes.loaders.PipesContextLoader;
import pipes.loaders.PipesDefaultContextLoader;
import pipes.loaders.PipesS3ContextLoader;
import pipes.writers.PipesDefaultMessageWriter;
import pipes.writers.PipesMessageWriter;
import pipes.writers.PipesMessageWriterChannel;
import pipes.writers.PipesS3MessageWriter;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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
        names = {"--context"},
        description = "Provide DAGSTER_PIPES_CONTEXT value for testing"
    )
    private String context;

    @CommandLine.Option(
        names = {"--messages"},
        description = "Provide DAGSTER_PIPES_MESSAGES value for testing"
    )
    private String messages;

    @CommandLine.Option(
        names = {"--env"},
        description = "Get DAGSTER_PIPES_MESSAGES & DAGSTER_PIPES_CONTEXT values " +
            "from environmental variables"
    )
    private boolean env = false;

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
        names = {"--full"},
        description = "Flag to test full PipesContext usage"
    )
    private boolean full = false;

    @CommandLine.Option(
        names = {"--custom-payload-path"},
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
        names = {"--throw-error"},
        description = "Throw exception in PipesSession with specified message"
    )
    private boolean throwException = false;

    @CommandLine.Option(
        names = {"--logging"},
        description = "Flag to test logging"
    )
    private boolean logging = false;

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

    @Override
    public void run() {
        Map<String, String> input = new HashMap<>();
        PipesTests pipesTests = new PipesTests();
        try {
            if (this.context != null) {
                input.put(PipesConstants.CONTEXT_ENV_VAR.name, context);
            }
            if (this.messages != null) {
                input.put(PipesConstants.MESSAGES_ENV_VAR.name, this.messages);
            }
            pipesTests.setInput(input);

            final PipesContextLoader loader;
            if (this.contextLoaderType != null && !this.contextLoaderType.isEmpty()) {
                switch (this.contextLoaderType) {
                    case "s3":
                        S3Client amazonS3Client = S3Client.builder().build();
                        loader = new PipesS3ContextLoader(amazonS3Client);
                        break;
                    case "default":
                        loader = new PipesDefaultContextLoader();
                        break;
                    default:
                        throw new IllegalArgumentException("Specified unknown context loader type!");
                }
                pipesTests.setContextLoader(loader);
            }

            final PipesMessageWriter<? extends PipesMessageWriterChannel> writer;
            if (this.messageWriter != null && !this.messageWriter.isEmpty()) {
                switch (this.messageWriter) {
                    case "s3":
                        S3Client amazonS3Client = S3Client.builder().build();
                        writer = new PipesS3MessageWriter(amazonS3Client);
                        break;
                    case "default":
                        writer = new PipesDefaultMessageWriter();
                        break;
                    default:
                        throw new IllegalArgumentException("Specified unknown message writer!");
                }
                pipesTests.setMessageWriter(writer);
            }

            // Setup payload if required
            if (this.customPayloadPath != null && !this.customPayloadPath.isEmpty()) {
                cacheJson(this.customPayloadPath);
                Object payload = loadParamByWrapperKey("payload", Object.class);
                pipesTests.setPayload(payload);
            }

            if (this.throwException) {
                pipesTests.testRunPipesSessionWithException();
                return;
            }

            if (this.logging) {
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
		PipesAssetCheckSeverity severity = PipesAssetCheckSeverity.valueOf(loadParamByWrapperKey("severity", String.class));
                pipesTests.setCheck(checkName, passed, assetKey, severity);
            }

            if (this.full) {
                pipesTests.fullTest();
                return;
            } else {
                pipesTests.setContextData();
            }

            if (this.extras != null) {
                File jsonFile = new File(this.extras);
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> extrasMap = objectMapper.readValue(
                    jsonFile, new TypeReference<Map<String, Object>>() {}
                );
                pipesTests.setExtras(extrasMap);
                pipesTests.testExtras();
            }

            if (this.jobName != null) {
                pipesTests.setJobName(this.jobName);
                pipesTests.testJobName();
            }
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
