package org.texttechnologylab.duui.refactor.storage;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface DUUIRegistry<K, V> {
    Entry<K, V> put(K key, V value);

    Optional<V> get(K key);

    V require(K key);

    Optional<V> remove(K key);

    boolean contains(K key);

    List<V> values();

    Set<K> keys();

    List<Entry<K, V>> entries();

    long size();

    void clear();

    record Entry<K, V>(K key, V value) {
    }
}
