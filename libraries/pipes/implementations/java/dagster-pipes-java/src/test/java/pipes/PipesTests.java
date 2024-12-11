package pipes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import pipes.data.PipesContextData;
import pipes.data.PipesMetadata;
import pipes.loaders.*;
import pipes.writers.*;
import types.Type;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("PMD.JUnit5TestShouldBePackagePrivate")
@Disabled
public class PipesTests {

    private Map<String, String> input;
    private PipesContextLoader contextLoader;
    private PipesContextData contextData;
    private Map<String, Object> extras;
    private String jobName;
    private Object payload;

    private Map<String, PipesMetadata> metadata;

    //Related to reportAssetMaterialization
    private boolean materialization;
    private String dataVersion;
    private String materializationAssetKey;

    //Related to reportAssetCheck
    private boolean check;
    private String checkName;
    private boolean passed;
    private String checkAssetKey;

    //Message writer
    private PipesMessageWriter<? extends PipesMessageWriterChannel> pipesMessageWriter;

    void setInput(Map<String, String> input) {
        this.input = input;
    }

    void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }

    void setJobName(String jobName) {
        this.jobName = jobName;
    }

    void setContextData() throws DagsterPipesException {
        this.contextData = DataLoader.getData(input);
    }

    void setContextLoader(PipesContextLoader contextLoader) throws DagsterPipesException {
        this.contextLoader = contextLoader;
    }


    void setPayload(Object payload) {
        this.payload = payload;
    }

    void setMaterialization(String dataVersion, String assetKey) {
        this.materialization = true;
        this.dataVersion = dataVersion;
        this.materializationAssetKey = assetKey;
    }

    void setCheck(String checkName, boolean passed, String assetKey) {
        this.check = true;
        this.checkName = checkName;
        this.passed = passed;
        this.checkAssetKey = assetKey;
    }

    void setMessageWriter(PipesMessageWriter<? extends PipesMessageWriterChannel> writer) {
        this.pipesMessageWriter = writer;
    }

    @Test
    void testExtras() {
        Assertions.assertTrue(
            contextData.getExtras().entrySet().containsAll(this.extras.entrySet()),
            "Extras does not contain all provided entries."
        );
        System.out.println("Extras are correct.");
    }

    @Test
    void testJobName() {
        Assertions.assertEquals(
            this.jobName,
            contextData.getJobName(),
            "JobName is incorrect."
        );
        System.out.println("JobName is correct.");
    }

    @Test
    void fullTest() throws DagsterPipesException {
        getTestSession().runDagsterPipes(this::fullTest);
    }

    private void fullTest(PipesContext context) throws DagsterPipesException {
        context.reportCustomMessage("Hello from Java!");

        if (this.payload != null) {
            context.reportCustomMessage(this.payload);
            System.out.println("Payload reported with custom message.");
        }

        if (this.materialization) {
            buildTestMetadata();
            context.reportAssetMaterialization(
                this.metadata, this.dataVersion, this.materializationAssetKey
            );
        }
        if (this.check) {
            buildTestMetadata();
            context.reportAssetCheck(
                this.checkName, this.passed, this.metadata, this.checkAssetKey
            );
        }
    }

    @Test
    void testRunPipesSessionWithException() throws DagsterPipesException {
        getTestSession().runDagsterPipes((context) -> {
            throw new DagsterPipesException("Very bad Java exception happened!");
        });
    }

    @Test
    void testLogging() throws DagsterPipesException {
        getTestSession().runDagsterPipes((context) -> {
            context.getLogger().debug("Debug message");
            context.getLogger().info("Info message");
            context.getLogger().warning("Warning message");
            context.getLogger().error("Error message");
            context.getLogger().critical("Critical message");
        });
    }

    public Map<String, PipesMetadata> buildTestMetadata() {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
            this.metadata.put("float", new PipesMetadata(0.1, Type.FLOAT));
            this.metadata.put("int", new PipesMetadata(1, Type.INT));
            this.metadata.put("text", new PipesMetadata("hello", Type.TEXT));
            this.metadata.put("notebook", new PipesMetadata("notebook.ipynb", Type.NOTEBOOK));
            this.metadata.put("path", new PipesMetadata("/dev/null", Type.PATH));
            this.metadata.put("md", new PipesMetadata("**markdown**", Type.MD));
            this.metadata.put("bool_true", new PipesMetadata(true, Type.BOOL));
            this.metadata.put("bool_false", new PipesMetadata(false, Type.BOOL));
            this.metadata.put("asset", new PipesMetadata("foo/bar", Type.ASSET));
            this.metadata.put(
                "dagster_run",
                new PipesMetadata("db892d7f-0031-4747-973d-22e8b9095d9d", Type.DAGSTER_RUN)
            );
            this.metadata.put("null", new PipesMetadata(null, Type.NULL));
            this.metadata.put("url", new PipesMetadata("https://dagster.io", Type.URL));
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("foo", "bar");
            jsonMap.put("baz", 1);
            jsonMap.put("qux", new int[]{1, 2, 3});
            Map<String, Integer> inner = new HashMap<>();
            inner.put("a", 1);
            inner.put("b", 2);
            jsonMap.put("quux", inner);
            jsonMap.put("corge", null);
            this.metadata.put("json", new PipesMetadata(jsonMap, Type.JSON));
        }
        return this.metadata;
    }

    private PipesSession getTestSession() throws DagsterPipesException {
        PipesParamsLoader paramsLoader = new PipesEnvVarParamsLoader();
        PipesContextLoader contextLoader = this.contextLoader == null
            ? new PipesDefaultContextLoader() : this.contextLoader;
        PipesMessageWriter<? extends PipesMessageWriterChannel> messageWriter = this.pipesMessageWriter;
        return new PipesSession(paramsLoader, contextLoader, messageWriter);
    }
}
