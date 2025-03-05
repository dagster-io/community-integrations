package io.dagster.pipes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.dagster.pipes.DagsterPipesException;
import io.dagster.pipes.PipesContext;
import io.dagster.pipes.PipesSession;
import io.dagster.pipes.data.PipesAssetCheckSeverity;
import io.dagster.pipes.data.PipesContextData;
import io.dagster.pipes.data.PipesMetadata;
import io.dagster.pipes.loaders.*;
import io.dagster.pipes.writers.*;
import io.dagster.types.Type;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("PMD.JUnit5TestShouldBePackagePrivate")
@Disabled
public class PipesTests {

    private PipesContextLoader contextLoader;
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
    private PipesAssetCheckSeverity severity;

    //Message writer
    private PipesMessageWriter<? extends PipesMessageWriterChannel> pipesMessageWriter;

    void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }

    void setJobName(String jobName) {
        this.jobName = jobName;
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

    void setCheck(String checkName, boolean passed, String assetKey, PipesAssetCheckSeverity severity) {
        this.check = true;
        this.checkName = checkName;
        this.passed = passed;
        this.checkAssetKey = assetKey;
        this.severity = severity;
    }

    void setMessageWriter(PipesMessageWriter<? extends PipesMessageWriterChannel> writer) {
        this.pipesMessageWriter = writer;
    }

    @Test
    void fullTest() throws DagsterPipesException {
        getTestSession().runDagsterPipes(this::fullTest);
    }

    private void fullTest(PipesContext context) throws DagsterPipesException {
        context.reportCustomMessage("Hello from external process!");

        if (this.extras != null) {
            Assertions.assertTrue(
                context.getExtras().entrySet().containsAll(this.extras.entrySet()),
                "Extras does not contain all provided entries."
            );
            System.out.println("Extras are correct.");
        }

        if (this.jobName != null) {
            Assertions.assertEquals(
                this.jobName,
                context.getJobName(),
                "JobName is incorrect."
            );
            System.out.println("JobName is correct.");
        }

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
                this.checkName, this.passed, this.severity, this.metadata, this.checkAssetKey
            );
        }
    }

    @Test
    void testRunPipesSessionWithException() throws DagsterPipesException {
        getTestSession().runDagsterPipes((context) -> {
            throw new DagsterPipesException("Very bad error has happened!");
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
