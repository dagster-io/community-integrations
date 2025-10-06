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


    /**
     * Reports an exception that should terminate execution. The exception will be
     * propagated to Dagster when the context is closed.
     *
     * @param exception Exception to report
     */
    void reportException(Exception exception);

    /**
     * Closes the context and flushes pending messages. This must be called to
     * ensure proper cleanup and exception reporting.
     *
     * @throws DagsterPipesException If an error occurs during close or if a
     *         previously reported exception exists
     */
    @Override
    void close() throws DagsterPipesException;

    /**
     * Reports a custom message payload.
     *
     * @param payload Custom data to report (will be serialized)
     * @throws DagsterPipesException If context is already closed
     */
    void reportCustomMessage(Object payload) throws DagsterPipesException;

    /**
     * Checks if the context has been closed.
     *
     * @return True or false
     */
    boolean isClosed();

    /**
     * Determines if the current step targets assets.
     *
     * @return True if asset keys are defined in the context, false otherwise
     */
    boolean isAssetStep();

    /**
     * Gets the single asset key for the current step. Valid only for single-asset steps.
     *
     * @return Single asset key
     * @throws DagsterPipesException If no assets or multiple assets are targeted
     */
    String getAssetKey() throws DagsterPipesException;

    /**
     * Gets all asset keys targeted by the current step.
     *
     * @return List of asset keys (non-empty)
     * @throws DagsterPipesException If no assets are targeted
     */
    List<String> getAssetKeys() throws DagsterPipesException;

    /**
     * Gets provenance data for the single asset in the current step.
     *
     * @return Provenance data for the asset
     * @throws DagsterPipesException If no assets or multiple assets are targeted
     */
    ProvenanceByAssetKey getProvenance() throws DagsterPipesException;

    /**
     * Gets provenance data by asset key.
     *
     * @return Map of asset keys to provenance data (non-empty)
     * @throws DagsterPipesException If no assets are targeted
     */
    Map<String, ProvenanceByAssetKey> getProvenanceByAssetKey() throws DagsterPipesException;

    /**
     * Gets code version for the single asset in the current step.
     *
     * @return Code version string
     * @throws DagsterPipesException If no assets or multiple assets are targeted
     */
    String getCodeVersion() throws DagsterPipesException;

    /**
     * Gets code versions keyed by asset.
     *
     * @return Map of asset keys to code versions (non-empty)
     * @throws DagsterPipesException If no assets are targeted
     */
    Map<String, String> getCodeVersionByAssetKey() throws DagsterPipesException;

    /**
     * Determines if the current step is partitioned.
     *
     * @return True if partition data exists in context, false otherwise
     */
    boolean isPartitionStep();

    /**
     * Gets the partition key for the current step.
     *
     * @return Current partition key
     * @throws DagsterPipesException If no partition is defined
     */
    String getPartitionKey() throws DagsterPipesException;

    /**
     * Gets the partition key range for the current step.
     *
     * @return Partition key range
     * @throws DagsterPipesException If no partition range is defined
     */
    PartitionKeyRange getPartitionKeyRange() throws DagsterPipesException;

    /**
     * Gets the partition time window for the current step.
     *
     * @return Partition time window
     * @throws DagsterPipesException If no time window is defined
     */
    PartitionTimeWindow getPartitionTimeWindow() throws DagsterPipesException;

    /**
     * Gets the Dagster run ID.
     *
     * @return Run ID string
     */
    String getRunId();

    /**
     * Gets the Dagster job name.
     *
     * @return Job name string
     */
    String getJobName();

    /**
     * Gets the current retry attempt number.
     *
     * @return Integer retry number
     */
    int getRetryNumber();


    /**
     * Gets a specific extra context parameter by key.
     *
     * @param key The key to retrieve
     * @return Parameter value
     * @throws DagsterPipesException If key is not found in extras
     */
    Object getExtra(String key) throws DagsterPipesException;

    /**
     * Gets all extra context parameters.
     *
     * @return Map of extra parameters
     */
    Map<String, Object> getExtras();

    /**
     * Gets the {@link PipesLogger} instance.
     *
     * @return The {@link PipesLogger} instance
     */
    PipesLogger getLogger();

    /**
     * Reports asset materialization.
     *
     * @param metadataMapping Metadata values keyed by label
     * @param dataVersion Data version string
     * @param assetKey Explicit asset key (null to use context's single asset)
     * @throws DagsterPipesException If asset key is invalid or context is closed
     * @throws IllegalStateException If asset has already been materialized
     */
    void reportAssetMaterialization(
            Map<String, ?> metadataMapping,
            String dataVersion,
            String assetKey
    ) throws DagsterPipesException;

    /**
     * Reports an asset check result (convenience method with default ERROR severity).
     *
     * @param checkName Unique check identifier
     * @param passed Whether the check succeeded
     * @param metadataMapping Metadata keyed by label
     * @param assetKey Explicit asset key (null to use context's single asset)
     * @throws DagsterPipesException If asset key is invalid or context is closed
     */
    void reportAssetCheck(
            String checkName,
            boolean passed,
            Map<String, ?> metadataMapping,
            String assetKey
    ) throws DagsterPipesException;

    /**
     * Reports an asset check result with custom severity.
     *
     * @param checkName Unique check identifier
     * @param passed Whether the check succeeded
     * @param severity Severity level
     * @param metadataMapping Metadata keyed by label
     * @param assetKey Explicit asset key (null to use context's single asset)
     * @throws DagsterPipesException If parameters are invalid or context is closed
     */
    void reportAssetCheck(
            String checkName,
            boolean passed,
            PipesAssetCheckSeverity severity,
            Map<String, ?> metadataMapping,
            String assetKey
    ) throws DagsterPipesException;
}

