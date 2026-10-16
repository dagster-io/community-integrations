package io.dagster.pipes;

import io.dagster.pipes.data.PipesAssetCheckSeverity;
import io.dagster.pipes.data.PipesContextData;
import io.dagster.pipes.loaders.PipesContextLoader;
import io.dagster.pipes.loaders.PipesParamsLoader;
import io.dagster.pipes.writers.PipesMessageWriter;
import io.dagster.pipes.writers.PipesMessageWriterChannel;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PipesContextImplTest {

    @Test
    void protocolMessagesAreWrittenOnlyThroughTheConfiguredChannel() throws Exception {
        final PrintStream originalOut = System.out;
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        final PrintStream capturedOut = new PrintStream(stdout);
        final List<PipesMessage> messages = new ArrayList<PipesMessage>();

        final PipesMessageWriterChannel channel = new PipesMessageWriterChannel() {
            @Override
            public void writeMessage(final PipesMessage message) {
                messages.add(message);
            }

            @Override
            public void close() {}
        };

        final PipesMessageWriter<PipesMessageWriterChannel> messageWriter =
            new PipesMessageWriter<PipesMessageWriterChannel>() {
                @Override
                public PipesMessageWriterChannel open(final Map<String, Object> params) {
                    return channel;
                }
            };

        final PipesParamsLoader paramsLoader = new PipesParamsLoader() {
            @Override
            public boolean isDagsterPipesProcess() {
                return true;
            }

            @Override
            public Optional<Map<String, Object>> loadContextParams() {
                return Optional.of(Collections.<String, Object>emptyMap());
            }

            @Override
            public Optional<Map<String, Object>> loadMessagesParams() {
                return Optional.of(Collections.<String, Object>emptyMap());
            }
        };

        final PipesContextLoader contextLoader = new PipesContextLoader() {
            @Override
            public PipesContextData loadContext(final Map<String, Object> params) {
                final PipesContextData data = new PipesContextData();
                data.setAssetKeys(Collections.singletonList("example_asset"));
                data.setExtras(Collections.<String, Object>emptyMap());
                return data;
            }
        };

        try {
            System.setOut(capturedOut);

            final PipesContextImpl context = new PipesContextImpl(
                paramsLoader, contextLoader, messageWriter
            );
            context.reportCustomMessage("hello");
            context.reportAssetMaterialization(
                Collections.<String, Object>emptyMap(), "v1", null
            );
            context.reportAssetCheck(
                "quality",
                true,
                PipesAssetCheckSeverity.ERROR,
                Collections.<String, Object>emptyMap(),
                null
            );
            context.close();

            Assertions.assertEquals("", stdout.toString("UTF-8"));
            // opened, custom message, materialization, asset check, closed
            Assertions.assertEquals(5, messages.size());
        } finally {
            System.setOut(originalOut);
            capturedOut.close();
        }
    }
}
