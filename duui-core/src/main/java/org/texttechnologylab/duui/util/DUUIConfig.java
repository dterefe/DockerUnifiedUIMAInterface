package org.texttechnologylab.duui.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public final class DUUIConfig<T> {
    private final Class<T> ownerType;
    private final Map<String, Object> values;

    private DUUIConfig(Builder<T> builder) {
        this.ownerType = builder.ownerType;
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(builder.values));
    }

    public static <T> Builder<T> builder(Class<T> ownerType) {
        return new Builder<>(ownerType);
    }

    public Class<T> ownerType() { return ownerType; }
    public Map<String, Object> values() { return values; }

    public Optional<Object> get(String key) { return Optional.ofNullable(values.get(key)); }

    public <V> Optional<V> get(String key, Class<V> valueType) {
        Object value = values.get(key);
        if (value == null || !valueType.isInstance(value)) return Optional.empty();
        return Optional.of(valueType.cast(value));
    }

    public <V> V getOrDefault(String key, Class<V> valueType, V defaultValue) {
        return get(key, valueType).orElse(defaultValue);
    }

    public static final class Builder<T> {
        private final Class<T> ownerType;
        private final Map<String, Object> values = new LinkedHashMap<>();

        private Builder(Class<T> ownerType) { this.ownerType = ownerType; }

        public Builder<T> set(String key, Object value) {
            if (key != null && value != null) values.put(key, value);
            return this;
        }

        public <V> Builder<T> setIf(String key, V value, Function<V, Boolean> predicate) {
            if (predicate != null && Boolean.TRUE.equals(predicate.apply(value))) set(key, value);
            return this;
        }

        public DUUIConfig<T> build() { return new DUUIConfig<>(this); }
    }
}
