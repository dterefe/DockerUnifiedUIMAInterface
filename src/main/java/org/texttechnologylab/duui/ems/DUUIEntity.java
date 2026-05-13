package org.texttechnologylab.duui.ems;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public abstract class DUUIEntity {
    private final String id;
    private final ConcurrentHashMap<String, Object> metadata = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();

    protected DUUIEntity(String id) {
        this.id = Objects.requireNonNullElseGet(id, () -> UUID.randomUUID().toString());
    }

    public String id() {
        return id;
    }

    public Map<String, Object> metadata() {
        return Map.copyOf(metadata);
    }

    public Map<String, Object> state() {
        return Map.copyOf(state);
    }

    public void metadata(String key, Object value) {
        write(metadata, key, value);
    }

    public void state(String key, Object value) {
        write(state, key, value);
    }

    public <T> Optional<T> metadata(String key, Class<T> type) {
        return read(metadata, key, type);
    }

    public <T> Optional<T> state(String key, Class<T> type) {
        return read(state, key, type);
    }

    private static void write(ConcurrentHashMap<String, Object> target, String key, Object value) {
        Objects.requireNonNull(key, "key");
        if (value == null) {
            target.remove(key);
            return;
        }
        target.put(key, value);
    }

    private static <T> Optional<T> read(ConcurrentHashMap<String, Object> source, String key, Class<T> type) {
        Objects.requireNonNull(type, "type");
        Object value = source.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (!type.isInstance(value)) {
            throw new IllegalStateException("Value for key '" + key + "' is not of type " + type.getName());
        }
        return Optional.of(type.cast(value));
    }
}
