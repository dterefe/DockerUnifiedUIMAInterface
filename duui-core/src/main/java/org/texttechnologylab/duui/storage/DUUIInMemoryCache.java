package org.texttechnologylab.duui.storage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Objects;
import java.util.Optional;

public final class DUUIInMemoryCache<K, V> implements DUUICache<K, V> {
    private final Cache<K, V> cache;
    private final Policy policy;

    public DUUIInMemoryCache() {
        this(Policy.unbounded());
    }

    public DUUIInMemoryCache(Policy policy) {
        this(build(policy), policy == null ? Policy.unbounded() : policy);
    }

    public DUUIInMemoryCache(Cache<K, V> cache, Policy policy) {
        this.cache = Objects.requireNonNull(cache, "cache");
        this.policy = policy == null ? Policy.unbounded() : policy;
    }

    @Override
    public Optional<V> get(K key) {
        return Optional.ofNullable(cache.getIfPresent(key));
    }

    @Override
    public V getOrLoad(K key, Loader<K, V> loader) {
        Objects.requireNonNull(loader, "loader");
        return cache.get(key, loader::load);
    }

    @Override
    public void put(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        cache.put(key, value);
    }

    @Override
    public void invalidate(K key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public long size() {
        cache.cleanUp();
        return cache.asMap().size();
    }

    @Override
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    @Override
    public Policy policy() {
        return policy;
    }

    private static <K, V> Cache<K, V> build(Policy policy) {
        Policy effective = policy == null ? Policy.unbounded() : policy;
        Caffeine<Object, Object> builder = Caffeine.newBuilder();
        if (effective.maximumSize() != null) {
            builder.maximumSize(effective.maximumSize());
        }
        if (effective.expireAfterWrite() != null) {
            builder.expireAfterWrite(effective.expireAfterWrite());
        }
        if (effective.expireAfterAccess() != null) {
            builder.expireAfterAccess(effective.expireAfterAccess());
        }
        return builder.build();
    }
}
