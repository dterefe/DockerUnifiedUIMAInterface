package org.texttechnologylab.duui.util;

import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class DUUIPolicy<K extends Enum<K>, V> {
    private final Class<K> keyType;
    private final V defaultValue;
    private final Map<K, V> values;
    private final Map<String, V> namedValues;

    private DUUIPolicy(Builder<K, V> builder) {
        this.keyType = builder.keyType;
        this.defaultValue = builder.defaultValue;
        this.values = Collections.unmodifiableMap(new EnumMap<>(builder.values));
        this.namedValues = Collections.unmodifiableMap(new LinkedHashMap<>(builder.namedValues));
    }

    public static <K extends Enum<K>, V> Builder<K, V> builder(Class<K> keyType, V defaultValue) {
        return new Builder<>(keyType, defaultValue);
    }

    public V resolve(K key) { return values.getOrDefault(key, defaultValue); }
    public V resolve(String name) { return namedValues.getOrDefault(name, defaultValue); }
    public Optional<V> specific(K key) { return Optional.ofNullable(values.get(key)); }
    public Optional<V> specific(String name) { return Optional.ofNullable(namedValues.get(name)); }
    public V defaultValue() { return defaultValue; }
    public Class<K> keyType() { return keyType; }

    public static final class Builder<K extends Enum<K>, V> {
        private final Class<K> keyType;
        private final V defaultValue;
        private final Map<K, V> values;
        private final Map<String, V> namedValues = new LinkedHashMap<>();

        private Builder(Class<K> keyType, V defaultValue) {
            this.keyType = keyType;
            this.defaultValue = defaultValue;
            this.values = new EnumMap<>(keyType);
        }

        public Builder<K, V> on(K key, V value) {
            if (key != null && value != null) values.put(key, value);
            return this;
        }

        public Builder<K, V> named(String name, V value) {
            if (name != null && value != null) namedValues.put(name, value);
            return this;
        }

        public DUUIPolicy<K, V> build() { return new DUUIPolicy<>(this); }
    }
}
