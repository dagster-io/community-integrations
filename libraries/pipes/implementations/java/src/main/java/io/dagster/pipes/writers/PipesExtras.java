package io.dagster.pipes.writers;

import java.util.HashMap;
import java.util.Map;

public class PipesExtras {

    private final Map<String, Object> extras;

    public PipesExtras() {
        this.extras = new HashMap<>();
    }

    public PipesExtras(final Map<String, Object> extras) {
        this.extras = extras != null ? extras : new HashMap<>();
    }

    public Map<String, Object> getExtras() {
        return extras;
    }

    public void addExtra(final String key, final Object value) {
        this.extras.put(key, value);
    }

    public Object getExtra(final String key) {
        return this.extras.get(key);
    }
}
