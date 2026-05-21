package org.texttechnologylab.duui.refactor.storage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class DUUIInMemoryRegistry<K, V> implements DUUIRegistry<K, V> {
    private final Cache<K, V> entries;

    public DUUIInMemoryRegistry() {
        this(Caffeine.newBuilder().build());
    }

    public DUUIInMemoryRegistry(Cache<K, V> entries) {
        this.entries = Objects.requireNonNull(entries, "entries");
    }

    @Override
    public Entry<K, V> put(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        entries.put(key, value);
        return new Entry<>(key, value);
    }

    @Override
    public Optional<V> get(K key) {
        return Optional.ofNullable(entries.getIfPresent(key));
    }

    @Override
    public V require(K key) {
        return get(key).orElseThrow(() -> new NoSuchElementException("No DUUI registry entry for key " + key));
    }

    @Override
    public Optional<V> remove(K key) {
        V value = entries.getIfPresent(key);
        entries.invalidate(key);
        return Optional.ofNullable(value);
    }

    @Override
    public boolean contains(K key) {
        return entries.getIfPresent(key) != null;
    }

    @Override
    public List<V> values() {
        return List.copyOf(entries.asMap().values());
    }

    @Override
    public Set<K> keys() {
        return Set.copyOf(entries.asMap().keySet());
    }

    @Override
    public List<Entry<K, V>> entries() {
        return entries.asMap().entrySet().stream()
                .map(entry -> new Entry<>(entry.getKey(), entry.getValue()))
                .toList();
    }

    @Override
    public long size() {
        entries.cleanUp();
        return entries.asMap().size();
    }

    @Override
    public void clear() {
        entries.invalidateAll();
    }
}
