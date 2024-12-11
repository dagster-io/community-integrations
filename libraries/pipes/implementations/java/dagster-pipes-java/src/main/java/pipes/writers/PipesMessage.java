package pipes.writers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.Map;

@SuppressWarnings({"PMD.DataClass", "PMD.AvoidThrowingRawExceptionTypes"})
@JsonPropertyOrder({"__dagster_pipes_version", "method", "params"})
public class PipesMessage {

    @JsonProperty("__dagster_pipes_version")
    private String dagsterPipesVersion;
    private String method;
    private Map<String, ?> params;

    public PipesMessage(String dagsterPipesVersion, String method, Map<String, ?> params) {
        this.dagsterPipesVersion = dagsterPipesVersion;
        this.method = method;
        this.params = params;
    }

    public String getDagsterPipesVersion() {
        return dagsterPipesVersion;
    }

    public void setDagsterPipesVersion(String dagsterPipesVersion) {
        this.dagsterPipesVersion = dagsterPipesVersion;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Map<String, ?> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public boolean hasParams() {
        return this.params != null;
    }

    @Override
    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        String result;
        try {
            result = objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
