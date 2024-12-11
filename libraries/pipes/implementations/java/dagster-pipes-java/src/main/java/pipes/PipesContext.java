package pipes;

import pipes.logger.PipesLogger;
import pipes.utils.MetadataBuilder;
import types.*;
import pipes.data.*;
import pipes.loaders.PipesContextLoader;
import pipes.loaders.PipesParamsLoader;
import pipes.utils.PipesUtils;
import pipes.writers.PipesMessageWriter;
import pipes.writers.PipesMessageWriterChannel;

import java.util.*;
import java.util.logging.Logger;

@SuppressWarnings({"PMD.GodClass", "PMD.TooManyMethods", "PMD.ImmutableField"})
public class PipesContext {

    private static PipesContext instance;
    private PipesContextData data;
    private PipesMessageWriterChannel messageChannel;
    private final Set<String> materializedAssets;
    private boolean closed;
    final PipesLogger logger;
    private Exception exception;

    public PipesContext(
        PipesParamsLoader paramsLoader,
        PipesContextLoader contextLoader,
        PipesMessageWriter<? extends PipesMessageWriterChannel> messageWriter
    ) throws DagsterPipesException {
        Optional<Map<String, Object>> contextParams = paramsLoader.loadContextParams();
        Optional<Map<String, Object>> messageParams = paramsLoader.loadMessagesParams();
        if (contextParams.isPresent() && messageParams.isPresent()) {
            this.data = contextLoader.loadContext(contextParams.get());
            this.messageChannel = messageWriter.open(messageParams.get());
            Map<String, Object> openedPayload = messageWriter.getOpenedPayload();
            this.messageChannel.writeMessage(PipesUtils.makeMessage(Method.OPENED, openedPayload));
        }
        this.materializedAssets = new HashSet<>();
        this.closed = false;
        this.logger = new PipesLogger(
            Logger.getLogger(PipesContext.class.getName()), this.messageChannel
        );
    }

    public void reportException(Exception exception) {
        this.exception = exception;
    }

    public void close() throws DagsterPipesException {
        if (!this.closed) {
            Map<String, Object> payload = new HashMap<>();
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

    public static boolean isInitialized() {
        return instance != null;
    }

    public static void set(PipesContext context) {
        instance = context;
    }

    public static PipesContext get() {
        if (instance == null) {
            throw new IllegalStateException(
                "PipesContext has not been initialized. You must call openDagsterPipes()."
            );
        }
        return instance;
    }

    public void reportCustomMessage(Object payload) throws DagsterPipesException {
        Map<String, Object> map = new HashMap<>();
        map.put("payload", payload);
        writeMessage(Method.REPORT_CUSTOM_MESSAGE, map);
    }

    private void writeMessage(
        Method method,
        Map<String, Object> params
    ) throws DagsterPipesException {
        if (this.closed) {
            throw new DagsterPipesException("Cannot send message after pipes context is closed.");
        }
        System.out.println(PipesUtils.makeMessage(method, params));
        this.messageChannel.writeMessage(PipesUtils.makeMessage(method, params));
    }

    public boolean isClosed() {
        return this.closed;
    }

    public boolean isAssetStep() {
        return this.data.getAssetKeys() != null;
    }

    public String getAssetKey() throws DagsterPipesException {
        List<String> assetKeys = getAssetKeys();
        assertSingleAsset(assetKeys, "Asset key");
        return assetKeys.get(0);
    }

    public List<String> getAssetKeys() throws DagsterPipesException {
        List<String> assetKeys = this.data.getAssetKeys();
        assertPresence(assetKeys, "Asset keys");
        return assetKeys;
    }

    public PipesDataProvenance getProvenance() throws DagsterPipesException {
        Map<String, PipesDataProvenance> provenanceByAssetKey = getProvenanceByAssetKey();
        assertSingleAsset(provenanceByAssetKey, "Provenance");
        return provenanceByAssetKey.values().iterator().next();
    }

    public Map<String, PipesDataProvenance> getProvenanceByAssetKey() throws DagsterPipesException {
        Map<String, PipesDataProvenance> provenanceByAssetKey = this.data.getProvenanceByAssetKey();
        assertPresence(provenanceByAssetKey, "Provenance by asset key");
        return provenanceByAssetKey;
    }

    public String getCodeVersion() throws DagsterPipesException {
        Map<String, String> codeVersionByAssetKey = getCodeVersionByAssetKey();
        assertSingleAsset(codeVersionByAssetKey, "Code version");
        return codeVersionByAssetKey.values().iterator().next();
    }

    public Map<String, String> getCodeVersionByAssetKey() throws DagsterPipesException {
        Map<String, String> codeVersionByAssetKey = this.data.getCodeVersionByAssetKey();
        assertPresence(codeVersionByAssetKey, "Code version by asset key");
        return codeVersionByAssetKey;
    }

    public boolean isPartitionStep() {
        return this.data.getPartitionKeyRange() != null;
    }

    public String getPartitionKey() throws DagsterPipesException {
        String partitionKey = this.data.getPartitionKey();
        assertPresence(partitionKey, "Partition key");
        return partitionKey;
    }

    public PartitionKeyRange getPartitionKeyRange() throws DagsterPipesException {
        PartitionKeyRange partitionKeyRange = this.data.getPartitionKeyRange();
        assertPresence(partitionKeyRange, "Partition key range");
        return partitionKeyRange;
    }

    public PartitionTimeWindow getPartitionTimeWindow() throws DagsterPipesException {
        PartitionTimeWindow partitionTimeWindow = this.data.getPartitionTimeWindow();
        assertPresence(partitionTimeWindow, "Partition time window");
        return partitionTimeWindow;
    }

    public String getRunId() {
        return this.data.getRunId();
    }

    public String getJobName() {
        return this.data.getJobName();
    }

    public int getRetryNumber() {
        return this.data.getRetryNumber();
    }

    public Object getExtra(String key) throws DagsterPipesException {
        Map<String, Object> extras = this.data.getExtras();
        if (!extras.containsKey(key)) {
            throw new DagsterPipesException(
                String.format("Extra %s is undefined. Extras must be provided by user.", key)
            );
        }
        return extras.get(key);
    }

    public Map<String, Object> getExtras() {
        return this.data.getExtras();
    }

    public PipesLogger getLogger() {
        return this.logger;
    }

    private static void assertSingleAsset(Collection<?> collection, String name) throws DagsterPipesException {
        if (collection.size() != 1) {
            throw new DagsterPipesException(
                String.format("%s is undefined. Current step targets multiple assets.", name)
            );
        }
    }

    private static void assertSingleAsset(Map<?, ?> map, String name) throws DagsterPipesException {
        if (map.size() != 1) {
            throw new DagsterPipesException(
                String.format("%s is undefined. Current step targets multiple assets.", name)
            );
        }
    }

    private static void assertPresence(Object object, String name) throws DagsterPipesException {
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

    public void reportAssetMaterialization(
        final Object simpleMetadataValue,
        final String dataVersion,
        final String assetKey
    ) throws DagsterPipesException {
        reportAssetMaterialization(MetadataBuilder.buildFrom(simpleMetadataValue), dataVersion, assetKey);
    }

    public void reportAssetMaterialization(
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

    public void reportAssetCheck(
        String checkName,
        boolean passed,
        Object simpleMetadataValue,
        String assetKey
    ) throws DagsterPipesException {
        reportAssetCheck(
            checkName, passed, PipesAssetCheckSeverity.ERROR,
            MetadataBuilder.buildFrom(simpleMetadataValue), assetKey
        );
    }

    public void reportAssetCheck(
        String checkName,
        boolean passed,
        Map<String, PipesMetadata> pipesMetadata,
        String assetKey
    ) throws DagsterPipesException {
        reportAssetCheck(
            checkName, passed, PipesAssetCheckSeverity.ERROR, pipesMetadata, assetKey
        );
    }

    public void reportAssetCheck(
        final String checkName,
        final boolean passed,
        final PipesAssetCheckSeverity severity,
        Object simpleMetadataValue,
        final String assetKey
    ) throws DagsterPipesException {
        System.out.println("was: " + checkName + " " + passed + " " + assetKey);
        assertNotNull(checkName, Method.REPORT_ASSET_CHECK, "checkName");
        final String actualAssetKey = resolveOptionallyPassedAssetKey(assetKey, Method.REPORT_ASSET_CHECK);
        System.out.println("resolved:" + actualAssetKey);
        this.writeMessage(
            Method.REPORT_ASSET_CHECK,
            this.createMap(
                actualAssetKey, checkName, passed, severity,
                MetadataBuilder.buildFrom(simpleMetadataValue)
            )
        );
    }

    public void reportAssetCheck(
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

    private void assertNotNull(Object value, Method method, String param) throws DagsterPipesException {
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
        String assetKey,
        String dataVersion,
        Map<String, PipesMetadata> pipesMetadata
    ) {
        Map<String, Object> message = new HashMap<>();
        message.put("asset_key", assetKey);
        message.put("data_version", dataVersion);
        message.put("metadata", pipesMetadata);
        return message;
    }

    private Map<String, Object> createMap(
        String assetKey,
        String checkName,
        boolean passed,
        PipesAssetCheckSeverity severity,
        Map<String, PipesMetadata> pipesMetadata
    ) {
        Map<String, Object> message = new HashMap<>();
        message.put("asset_key", assetKey);
        message.put("check_name", checkName);
        message.put("passed", passed);
        message.put("severity", severity);
        message.put("metadata", pipesMetadata);
        return message;
    }

    private String resolveOptionallyPassedAssetKey(
        String assetKey,
        Method method
    ) throws DagsterPipesException {
        List<String> definedAssetKeys = this.data.getAssetKeys();
        String resultAssetKey = assetKey;
        if (assetKey == null) {
            if (definedAssetKeys.size() != 1) {
                throw new DagsterPipesException(
                    String.format(
                        "Calling %s without passing an asset key is undefined. Current step targets multiple assets.",
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
                    "Calling %s without passing an asset key is undefined. Current step does not target a specific asset.",
                    method.toValue()
                )
            );
        }

        return resultAssetKey;
    }
}