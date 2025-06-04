package io.dagster.pipes.writers;

import io.dagster.pipes.DagsterPipesException;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for pipes message writers. Handles:
 * <ul>
 *   <li>Opening communication channels ({@link PipesMessageWriterChannel})</li>
 *   <li>Constructing opened message payloads</li>
 *   <li>Providing extension points for custom payload data</li>
 * </ul>
 */
public abstract class PipesMessageWriter<T extends PipesMessageWriterChannel> {
    /**
     * Opens a message channel for pipes communication. This is the primary extension point
     * for concrete implementations to establish their specific communication mechanism.
     *
     * @param params Configuration parameters for channel initialization
     * @return Initialized message channel
     * @throws DagsterPipesException If channel initialization fails due to invalid
     *         parameters or connection issues
     */
    public abstract T open(Map<String, Object> params) throws DagsterPipesException;

    /**
     * Builds the standardized payload for opened messages.
     *
     * <p>This final method should not be overridden. For custom payload data,
     * implement {@link #getOpenedExtras()} instead.
     *
     * @return The opened message payload.
     */
    public final Map<String, Object> getOpenedPayload() {
        final Map<String, Object> payload = new HashMap<>();
        payload.put("extras", getOpenedExtras());
        return payload;
    }

    /**
     * Provides custom extras to include in opened messages.
     *
     * @return The map to include in opened payload
     */
    public Map<String, Object> getOpenedExtras() {
        return new HashMap<>();
    }
}
