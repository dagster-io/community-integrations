package io.dagster.pipes.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dagster.types.PartitionKeyRange;
import io.dagster.types.PartitionTimeWindow;
import io.dagster.types.ProvenanceByAssetKey;
import java.util.List;
import java.util.Map;

@SuppressWarnings("PMD.DataClass")
public class PipesContextData {

    @JsonProperty("asset_keys")
    private List<String> assetKeys;

    @JsonProperty("code_version_by_asset_key")
    private Map<String, String> codeVersionByAssetKey;

    @JsonProperty("provenance_by_asset_key")
    private Map<String, ProvenanceByAssetKey> provenanceByAssetKey;

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

    public List<String> getAssetKeys() {
        return assetKeys;
    }

    public void setAssetKeys(final List<String> assetKeys) {
        this.assetKeys = assetKeys;
    }

    public Map<String, String> getCodeVersionByAssetKey() {
        return codeVersionByAssetKey;
    }

    public void setCodeVersionByAssetKey(final Map<String, String> codeVersionByAssetKey) {
        this.codeVersionByAssetKey = codeVersionByAssetKey;
    }

    public Map<String, ProvenanceByAssetKey> getProvenanceByAssetKey() {
        return provenanceByAssetKey;
    }

    public void setProvenanceByAssetKey(final Map<String, ProvenanceByAssetKey> provenanceByAssetKey) {
        this.provenanceByAssetKey = provenanceByAssetKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(final String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public PartitionKeyRange getPartitionKeyRange() {
        return partitionKeyRange;
    }

    public void setPartitionKeyRange(final PartitionKeyRange partitionKeyRange) {
        this.partitionKeyRange = partitionKeyRange;
    }

    public PartitionTimeWindow getPartitionTimeWindow() {
        return partitionTimeWindow;
    }

    public void setPartitionTimeWindow(final PartitionTimeWindow partitionTimeWindow) {
        this.partitionTimeWindow = partitionTimeWindow;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(final String runId) {
        this.runId = runId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(final String jobName) {
        this.jobName = jobName;
    }

    public int getRetryNumber() {
        return retryNumber;
    }

    public void setRetryNumber(final int retryNumber) {
        this.retryNumber = retryNumber;
    }

    public Map<String, Object> getExtras() {
        return extras;
    }

    public void setExtras(final Map<String, Object> extras) {
        this.extras = extras;
    }
}
