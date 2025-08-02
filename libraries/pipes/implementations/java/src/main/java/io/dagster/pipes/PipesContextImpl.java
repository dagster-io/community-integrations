package io.dagster.pipes;

import io.dagster.pipes.data.PipesAssetCheckSeverity;
import io.dagster.pipes.data.PipesContextData;
import io.dagster.pipes.data.PipesException;
import io.dagster.pipes.data.PipesMetadata;
import io.dagster.pipes.loaders.PipesContextLoader;
import io.dagster.pipes.loaders.PipesParamsLoader;
import io.dagster.pipes.logger.PipesLogger;
import io.dagster.pipes.utils.PipesUtils;
import io.dagster.pipes.writers.PipesMessageWriter;
import io.dagster.pipes.writers.PipesMessageWriterChannel;
import io.dagster.types.Method;
import io.dagster.types.PartitionKeyRange;
import io.dagster.types.PartitionTimeWindow;
import io.dagster.types.ProvenanceByAssetKey;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Provides execution context and communication mechanisms for Dagster pipes.
 * This singleton class manages asset materialization tracking, message writing,
 * and context data access for external execution environments.
 *
 * <p>Key responsibilities include:
 * <ul>
 *   <li>Initializing and managing communication channels with Dagster</li>
 *   <li>Tracking asset materialization state</li>
 *   <li>Providing access to execution context (assets, partitions, run info)</li>
 *   <li>Reporting results and exceptions back to Dagster</li>
 * </ul>
 */
@SuppressWarnings({"PMD.GodClass", "PMD.TooManyMethods", "PMD.ImmutableField"})
public class PipesContextImpl implements PipesContext {

    private PipesContextData data;
    private PipesMessageWriterChannel messageChannel;
    private final Set<String> materializedAssets;
    private boolean closed;
    private final PipesLogger logger;
    private Exception exception;

    /**
     * Constructor.
     *
     * @param paramsLoader Loader for context and message parameters
     * @param contextLoader Loader for deserializing context data
     * @param messageWriter Writer for opening message channels
     * @throws DagsterPipesException If initialization fails or required parameters are missing
     */
    public PipesContextImpl(
        final PipesParamsLoader paramsLoader,
        final PipesContextLoader contextLoader,
        final PipesMessageWriter<? extends PipesMessageWriterChannel> messageWriter
    ) throws DagsterPipesException {
        final Optional<Map<String, Object>> contextParams = paramsLoader.loadContextParams();
        final Optional<Map<String, Object>> messageParams = paramsLoader.loadMessagesParams();
        if (contextParams.isPresent() && messageParams.isPresent()) {
            this.data = contextLoader.loadContext(contextParams.get());
            this.messageChannel = messageWriter.open(messageParams.get());
            final Map<String, Object> openedPayload = messageWriter.getOpenedPayload();
            this.messageChannel.writeMessage(PipesUtils.makeMessage(Method.OPENED, openedPayload));
        }
        this.materializedAssets = new HashSet<>();
        this.closed = false;
        this.logger = new PipesLogger(
            Logger.getLogger(PipesContextImpl.class.getName()), this.messageChannel
        );
    }

    @Override
    public void reportException(final Exception exception) {
        this.exception = exception;
    }

    @Override
    public void close() throws DagsterPipesException {
        if (!this.closed) {
            final Map<String, Object> payload = new HashMap<>();
            if (this.exception != null) {
                payload.put("exception", new PipesException(exception));
            }
            this.messageChannel.writeMessage(PipesUtils.makeMessage(Method.CLOSED, payload));
            this.messageChannel.close();
            this.closed = true;
            if (this.exception != null) {
                throw new DagsterPipesException("Exception in PipesSession", this.exception);
            }
        }
    }

    @Override
    public void reportCustomMessage(final Object payload) throws DagsterPipesException {
        final Map<String, Object> map = new HashMap<>();
        map.put("payload", payload);
        writeMessage(Method.REPORT_CUSTOM_MESSAGE, map);
    }

    private void writeMessage(
        final Method method,
        final Map<String, Object> params
    ) throws DagsterPipesException {
        if (this.closed) {
            throw new DagsterPipesException("Cannot send message after pipes context is closed.");
        }
        System.out.println(PipesUtils.makeMessage(method, params));
        this.messageChannel.writeMessage(PipesUtils.makeMessage(method, params));
    }


    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public boolean isAssetStep() {
        return this.data.getAssetKeys() != null;
    }

    @Override
    public String getAssetKey() throws DagsterPipesException {
        final List<String> assetKeys = getAssetKeys();
        assertSingleAsset(assetKeys, "Asset key");
        return assetKeys.get(0);
    }

    @Override
    public List<String> getAssetKeys() throws DagsterPipesException {
        final List<String> assetKeys = this.data.getAssetKeys();
        assertPresence(assetKeys, "Asset keys");
        return assetKeys;
    }

    @Override
    public ProvenanceByAssetKey getProvenance() throws DagsterPipesException {
        final Map<String, ProvenanceByAssetKey> provenanceByAssetKey = getProvenanceByAssetKey();
        assertSingleAsset(provenanceByAssetKey, "Provenance");
        return provenanceByAssetKey.values().iterator().next();
    }

    @Override
    public Map<String, ProvenanceByAssetKey> getProvenanceByAssetKey() throws DagsterPipesException {
        final Map<String, ProvenanceByAssetKey> provenanceByAssetKey = this.data.getProvenanceByAssetKey();
        assertPresence(provenanceByAssetKey, "Provenance by asset key");
        return provenanceByAssetKey;
    }

    @Override
    public String getCodeVersion() throws DagsterPipesException {
        final Map<String, String> codeVersionByAssetKey = getCodeVersionByAssetKey();
        assertSingleAsset(codeVersionByAssetKey, "Code version");
        return codeVersionByAssetKey.values().iterator().next();
    }

    @Override
    public Map<String, String> getCodeVersionByAssetKey() throws DagsterPipesException {
        final Map<String, String> codeVersionByAssetKey = this.data.getCodeVersionByAssetKey();
        assertPresence(codeVersionByAssetKey, "Code version by asset key");
        return codeVersionByAssetKey;
    }


    @Override
    public boolean isPartitionStep() {
        return this.data.getPartitionKeyRange() != null;
    }

    @Override
    public String getPartitionKey() throws DagsterPipesException {
        final String partitionKey = this.data.getPartitionKey();
        assertPresence(partitionKey, "Partition key");
        return partitionKey;
    }

    @Override
    public PartitionKeyRange getPartitionKeyRange() throws DagsterPipesException {
        final PartitionKeyRange partitionKeyRange = this.data.getPartitionKeyRange();
        assertPresence(partitionKeyRange, "Partition key range");
        return partitionKeyRange;
    }

    @Override
    public PartitionTimeWindow getPartitionTimeWindow() throws DagsterPipesException {
        final PartitionTimeWindow partitionTimeWindow = this.data.getPartitionTimeWindow();
        assertPresence(partitionTimeWindow, "Partition time window");
        return partitionTimeWindow;
    }

    @Override
    public String getRunId() {
        return this.data.getRunId();
    }

    @Override
    public String getJobName() {
        return this.data.getJobName();
    }

    @Override
    public int getRetryNumber() {
        return this.data.getRetryNumber();
    }

    @Override
    public Object getExtra(final String key) throws DagsterPipesException {
        final Map<String, Object> extras = this.data.getExtras();
        if (!extras.containsKey(key)) {
            throw new DagsterPipesException(
                String.format("Extra %s is undefined. Extras must be provided by user.", key)
            );
        }
        return extras.get(key);
    }

    @Override
    public Map<String, Object> getExtras() {
        return this.data.getExtras();
    }

    @Override
    public PipesLogger getLogger() {
        return this.logger;
    }

    private static void assertSingleAsset(
        final Collection<?> collection,
        final String name
    ) throws DagsterPipesException {
        if (collection.size() != 1) {
            throw new DagsterPipesException(
                String.format("%s is undefined. Current step targets multiple assets.", name)
            );
        }
    }

    private static void assertSingleAsset(final Map<?, ?> map, final String name) throws DagsterPipesException {
        if (map.size() != 1) {
            throw new DagsterPipesException(
                String.format("%s is undefined. Current step targets multiple assets.", name)
            );
        }
    }

    private static void assertPresence(final Object object, final String name) throws DagsterPipesException {
        if (object == null) {
            throw new DagsterPipesException(
                String.format("%s is undefined. Current step does not target an asset.", name)
            );
        }
        if (object instanceof Collection<?> && ((Collection<?>) object).isEmpty()) {
            throw new DagsterPipesException(
                String.format("%s is empty. Current step does not target an asset.", name)
            );
        }
    }

    @Override
    public void reportAssetMaterialization(
            final Map<String, ?> metadataMapping,
            final String dataVersion,
            final String assetKey
    ) throws DagsterPipesException {
        final Map<String, PipesMetadata> resolvedMetadata = PipesUtils.resolveMetadataMapping(metadataMapping);
        processAssetMaterialization(resolvedMetadata, dataVersion, assetKey);
    }

    private void processAssetMaterialization(
        final Map<String, PipesMetadata> pipesMetadata,
        final String dataVersion,
        final String assetKey
    ) throws DagsterPipesException {
        final String actualAssetKey = resolveOptionallyPassedAssetKey(assetKey, Method.REPORT_ASSET_MATERIALIZATION);
        if (this.materializedAssets.contains(actualAssetKey)) {
            throw new IllegalStateException(
                "Asset keys: " + actualAssetKey + " has already been materialized, cannot report additional data."
            );
        }
        System.out.println("writing message...");
        this.writeMessage(
            Method.REPORT_ASSET_MATERIALIZATION,
            this.createMap(actualAssetKey, dataVersion, pipesMetadata)
        );
        materializedAssets.add(actualAssetKey);
    }

    @Override
    public void reportAssetCheck(
            final String checkName,
            final boolean passed,
            final Map<String, ?> metadataMapping,
            final String assetKey
    ) throws DagsterPipesException {
        reportAssetCheck(checkName, passed, PipesAssetCheckSeverity.ERROR, metadataMapping, assetKey);
    }

    @Override
    public void reportAssetCheck(
            final String checkName,
            final boolean passed,
            final PipesAssetCheckSeverity severity,
            final Map<String, ?> metadataMapping,
            final String assetKey
    ) throws DagsterPipesException {
        final Map<String, PipesMetadata> resolvedMetadata = PipesUtils
            .resolveMetadataMapping(metadataMapping);
        processAssetCheck(checkName, passed, severity, resolvedMetadata, assetKey);
    }

    private void processAssetCheck(
        final String checkName,
        final boolean passed,
        final PipesAssetCheckSeverity severity,
        final Map<String, PipesMetadata> pipesMetadata,
        final String assetKey
    ) throws DagsterPipesException {
        System.out.println("was: " + checkName + " " + passed + " " + assetKey);
        assertNotNull(checkName, Method.REPORT_ASSET_CHECK, "checkName");
        final String actualAssetKey = resolveOptionallyPassedAssetKey(assetKey, Method.REPORT_ASSET_CHECK);
        System.out.println("resolved:" + actualAssetKey);
        this.writeMessage(
            Method.REPORT_ASSET_CHECK,
            this.createMap(actualAssetKey, checkName, passed, severity, pipesMetadata)
        );
    }

    private void assertNotNull(
        final Object value,
        final Method method,
        final String param
    ) throws DagsterPipesException {
        if (value == null) {
            throw new DagsterPipesException(
                String.format(
                    "Null parameter `%s` for %s",
                    param, method.toValue()
                )
            );
        }
    }

    private Map<String, Object> createMap(
        final String assetKey,
        final String dataVersion,
        final Map<String, PipesMetadata> pipesMetadata
    ) {
        final Map<String, Object> message = new HashMap<>();
        message.put("asset_key", assetKey);
        message.put("data_version", dataVersion);
        message.put("metadata", pipesMetadata);
        return message;
    }

    private Map<String, Object> createMap(
        final String assetKey,
        final String checkName,
        final boolean passed,
        final PipesAssetCheckSeverity severity,
        final Map<String, PipesMetadata> pipesMetadata
    ) {
        final Map<String, Object> message = new HashMap<>();
        message.put("asset_key", assetKey);
        message.put("check_name", checkName);
        message.put("passed", passed);
        message.put("severity", severity);
        message.put("metadata", pipesMetadata);
        return message;
    }

    private String resolveOptionallyPassedAssetKey(
        final String assetKey,
        final Method method
    ) throws DagsterPipesException {
        final List<String> definedAssetKeys = this.data.getAssetKeys();
        String resultAssetKey = assetKey;
        if (assetKey == null) {
            if (definedAssetKeys.size() != 1) {
                throw new DagsterPipesException(
                    String.format(
                        "Calling %s without passing an asset key is undefined. "
                            + "Current step targets multiple assets.",
                        method.toValue()
                    )
                );
            }
            resultAssetKey = definedAssetKeys.get(0);
        } else {
            if (!definedAssetKeys.contains(assetKey)) {
                throw new DagsterPipesException(
                    String.format("Invalid asset key. Expected one of %s, got %s.",
                        definedAssetKeys,
                        assetKey
                    )
                );
            }
        }

        if (resultAssetKey.isEmpty()) {
            throw new DagsterPipesException(
                String.format(
                    "Calling %s without passing an asset key is undefined. "
                        + "Current step does not target a specific asset.",
                    method.toValue()
                )
            );
        }

        return resultAssetKey;
    }
}
