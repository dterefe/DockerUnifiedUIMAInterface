package org.texttechnologylab.duui.refactor.storage;

import java.time.Duration;
import java.util.Optional;

public interface DUUICache<K, V> {
    Optional<V> get(K key);

    V getOrLoad(K key, Loader<K, V> loader);

    void put(K key, V value);

    void invalidate(K key);

    void invalidateAll();

    long size();

    long estimatedSize();

    Policy policy();

    @FunctionalInterface
    interface Loader<K, V> {
        V load(K key);
    }

    record Policy(Long maximumSize, Duration expireAfterWrite, Duration expireAfterAccess) {
        public static Policy unbounded() {
            return new Policy(null, null, null);
        }

        public static Policy maximumSize(long maximumSize) {
            return new Policy(maximumSize, null, null);
        }
    }
}
