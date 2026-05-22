package org.texttechnologylab.duui.storage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class DUUIInMemoryIndex<K, V> implements DUUIIndex<K, V> {
    private final Cache<K, List<V>> entries;

    public DUUIInMemoryIndex() {
        this(Caffeine.newBuilder().build());
    }

    public DUUIInMemoryIndex(Cache<K, List<V>> entries) {
        this.entries = Objects.requireNonNull(entries, "entries");
    }

    @Override
    public Entry<K, V> add(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        entries.asMap().compute(key, (ignored, existing) -> {
            List<V> values = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
            values.add(value);
            return List.copyOf(values);
        });
        return new Entry<>(key, value);
    }

    @Override
    public boolean remove(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        List<V> existing = entries.getIfPresent(key);
        if (existing == null || !existing.contains(value)) {
            return false;
        }
        List<V> values = new ArrayList<>(existing);
        boolean removed = values.remove(value);
        if (values.isEmpty()) {
            entries.invalidate(key);
        } else {
            entries.put(key, List.copyOf(values));
        }
        return removed;
    }

    @Override
    public List<V> remove(K key) {
        List<V> existing = entries.getIfPresent(key);
        entries.invalidate(key);
        return existing == null ? List.of() : List.copyOf(existing);
    }

    @Override
    public List<V> find(K key) {
        List<V> values = entries.getIfPresent(key);
        return values == null ? List.of() : List.copyOf(values);
    }

    @Override
    public List<Entry<K, V>> entries() {
        return entries.asMap().entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(value -> new Entry<>(entry.getKey(), value)))
                .toList();
    }

    @Override
    public boolean contains(K key) {
        return entries.getIfPresent(key) != null;
    }

    @Override
    public long size() {
        return entries().size();
    }

    @Override
    public void clear() {
        entries.invalidateAll();
    }
}
