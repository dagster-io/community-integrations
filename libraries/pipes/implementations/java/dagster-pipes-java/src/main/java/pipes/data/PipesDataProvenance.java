package pipes.data;

import java.util.Map;

@SuppressWarnings("PMD")
public class PipesDataProvenance {

    private String codeVersion;
    private Map<String, String> inputDataVersions;
    private boolean userProvided;

    public PipesDataProvenance(
        String codeVersion,
        Map<String, String> inputDataVersions,
        boolean userProvided
    ) {
        this.codeVersion = codeVersion;
        this.inputDataVersions = inputDataVersions;
        this.userProvided = userProvided;
    }

    public String getCodeVersion() {
        return codeVersion;
    }

    public void setCodeVersion(String codeVersion) {
        this.codeVersion = codeVersion;
    }

    public Map<String, String> getInputDataVersions() {
        return inputDataVersions;
    }

    public void setInputDataVersions(Map<String, String> inputDataVersions) {
        this.inputDataVersions = inputDataVersions;
    }

    public boolean isUserProvided() {
        return userProvided;
    }

    public void setUserProvided(boolean userProvided) {
        this.userProvided = userProvided;
    }
}