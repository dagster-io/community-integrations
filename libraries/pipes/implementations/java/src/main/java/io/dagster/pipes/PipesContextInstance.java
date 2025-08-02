package io.dagster.pipes;

import io.dagster.pipes.data.PipesAssetCheckSeverity;
import io.dagster.pipes.logger.PipesLogger;
import io.dagster.types.PartitionKeyRange;
import io.dagster.types.PartitionTimeWindow;
import io.dagster.types.ProvenanceByAssetKey;
import java.util.Collections;
import java.util.List;
import java.util.Map;

final class PipesContextInstance {
    private static PipesContext instance;

    private PipesContextInstance() {
    }

    /* default */ static boolean isInitialized() {
        return instance != null;
    }

    /* default */ static void set(final PipesContext context) {
        instance = context;
    }

    /* default */ static PipesContext get() {
        if (instance == null) {
            throw new IllegalStateException(
                    "PipesContext has not been initialized. You must call openDagsterPipes()."
            );
        }
        return instance;
    }

    /* default */ static final PipesContext NOOP = new PipesContext() {
        @Override
        public void reportException(final Exception exception) {

        }

        @Override
        public void close() {

        }

        @Override
        public void reportCustomMessage(final Object payload) {
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public boolean isAssetStep() {
            return false;
        }

        @Override
        public String getAssetKey() {
            return null;
        }

        @Override
        public List<String> getAssetKeys() {
            return Collections.emptyList();
        }

        @Override
        public ProvenanceByAssetKey getProvenance() {
            return null;
        }

        @Override
        public Map<String, ProvenanceByAssetKey> getProvenanceByAssetKey() {
            return Collections.emptyMap();
        }

        @Override
        public String getCodeVersion() {
            return null;
        }

        @Override
        public Map<String, String> getCodeVersionByAssetKey() {
            return Collections.emptyMap();
        }

        @Override
        public boolean isPartitionStep() {
            return false;
        }

        @Override
        public String getPartitionKey() {
            return null;
        }

        @Override
        public PartitionKeyRange getPartitionKeyRange() {
            return null;
        }

        @Override
        public PartitionTimeWindow getPartitionTimeWindow() {
            return null;
        }

        @Override
        public String getRunId() {
            return null;
        }

        @Override
        public String getJobName() {
            return null;
        }

        @Override
        public int getRetryNumber() {
            return 0;
        }

        @Override
        public Object getExtra(final String key) {
            return null;
        }

        @Override
        public Map<String, Object> getExtras() {
            return Collections.emptyMap();
        }

        @Override
        public PipesLogger getLogger() {
            return null;
        }

        @Override
        public void reportAssetMaterialization(final Map<String, ?> metadataMapping, final String dataVersion,
                                               final String assetKey) {

        }

        @Override
        public void reportAssetCheck(final String checkName, final boolean passed,
                                     final Map<String, ?> metadataMapping, final String assetKey) {

        }

        @Override
        public void reportAssetCheck(final String checkName, final boolean passed,
                                     final PipesAssetCheckSeverity severity, final Map<String, ?> metadataMapping,
                                     final String assetKey) {
        }
    };
}
