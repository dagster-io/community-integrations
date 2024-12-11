package pipes.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import types.PartitionKeyRange;
import types.PartitionTimeWindow;

import java.util.List;
import java.util.Map;

@SuppressWarnings("PMD")
public class PipesContextData {

    @JsonProperty("asset_keys")
    private List<String> assetKeys;

    @JsonProperty("code_version_by_asset_key")
    private Map<String, String> codeVersionByAssetKey;

    @JsonProperty("provenance_by_asset_key")
    private Map<String, PipesDataProvenance> provenanceByAssetKey;

    @JsonProperty("partition_key")
    private String partitionKey;

    @JsonProperty("partition_key_range")
    private PartitionKeyRange partitionKeyRange;

    @JsonProperty("partition_time_window")
    private PartitionTimeWindow partitionTimeWindow;

    @JsonProperty("run_id")
    private String runId;

    @JsonProperty("job_name")
    private String jobName;

    @JsonProperty("retry_number")
    private int retryNumber;

    @JsonProperty("extras")
    private Map<String, Object> extras;  // Required

    public PipesContextData() {
    }

    public List<String> getAssetKeys() {
        return assetKeys;
    }

    public void setAssetKeys(List<String> assetKeys) {
        this.assetKeys = assetKeys;
    }

    public Map<String, String> getCodeVersionByAssetKey() {
        return codeVersionByAssetKey;
    }

    public void setCodeVersionByAssetKey(Map<String, String> codeVersionByAssetKey) {
        this.codeVersionByAssetKey = codeVersionByAssetKey;
    }

    public Map<String, PipesDataProvenance> getProvenanceByAssetKey() {
        return provenanceByAssetKey;
    }

    public void setProvenanceByAssetKey(Map<String, PipesDataProvenance> provenanceByAssetKey) {
        this.provenanceByAssetKey = provenanceByAssetKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public PartitionKeyRange getPartitionKeyRange() {
        return partitionKeyRange;
    }

    public void setPartitionKeyRange(PartitionKeyRange partitionKeyRange) {
        this.partitionKeyRange = partitionKeyRange;
    }

    public PartitionTimeWindow getPartitionTimeWindow() {
        return partitionTimeWindow;
    }

    public void setPartitionTimeWindow(PartitionTimeWindow partitionTimeWindow) {
        this.partitionTimeWindow = partitionTimeWindow;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public int getRetryNumber() {
        return retryNumber;
    }

    public void setRetryNumber(int retryNumber) {
        this.retryNumber = retryNumber;
    }

    public Map<String, Object> getExtras() {
        return extras;
    }

    public void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }
}