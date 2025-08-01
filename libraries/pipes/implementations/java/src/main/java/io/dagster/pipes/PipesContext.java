package io.dagster.pipes;

import io.dagster.pipes.data.PipesAssetCheckSeverity;
import io.dagster.pipes.logger.PipesLogger;
import io.dagster.types.PartitionKeyRange;
import io.dagster.types.PartitionTimeWindow;
import io.dagster.types.ProvenanceByAssetKey;
import java.util.List;
import java.util.Map;


public interface PipesContext extends AutoCloseable {
    /**
     * Checks if the context has been initialized.
     *
     * @return True or false
     */
    static boolean isInitialized() {
        return PipesContextInstance.isInitialized();
    }

    /**
     * Sets the global context instance (singleton pattern).
     *
     * @param context Context to set
     */
    static void set(PipesContext context) {
        PipesContextInstance.set(context);
    }

    /**
     * Retrieves the global context instance.
     *
     * @return Initialized context instance
     * @throws IllegalStateException If context has not been initialized via {@link #set(PipesContext)}
     */
    static PipesContext get() {
        return PipesContextInstance.get();
    }

    void reportException(Exception exception);

    @Override
    void close() throws DagsterPipesException;

    void reportCustomMessage(Object payload) throws DagsterPipesException;

    boolean isClosed();

    boolean isAssetStep();

    String getAssetKey() throws DagsterPipesException;

    List<String> getAssetKeys() throws DagsterPipesException;

    ProvenanceByAssetKey getProvenance() throws DagsterPipesException;

    Map<String, ProvenanceByAssetKey> getProvenanceByAssetKey() throws DagsterPipesException;

    String getCodeVersion() throws DagsterPipesException;

    Map<String, String> getCodeVersionByAssetKey() throws DagsterPipesException;

    boolean isPartitionStep();

    String getPartitionKey() throws DagsterPipesException;

    PartitionKeyRange getPartitionKeyRange() throws DagsterPipesException;

    PartitionTimeWindow getPartitionTimeWindow() throws DagsterPipesException;

    String getRunId();

    String getJobName();

    int getRetryNumber();

    Object getExtra(String key) throws DagsterPipesException;

    Map<String, Object> getExtras();

    PipesLogger getLogger();

    void reportAssetMaterialization(
            Map<String, ?> metadataMapping,
            String dataVersion,
            String assetKey
    ) throws DagsterPipesException;

    void reportAssetCheck(
            String checkName,
            boolean passed,
            Map<String, ?> metadataMapping,
            String assetKey
    ) throws DagsterPipesException;

    void reportAssetCheck(
            String checkName,
            boolean passed,
            PipesAssetCheckSeverity severity,
            Map<String, ?> metadataMapping,
            String assetKey
    ) throws DagsterPipesException;
}

