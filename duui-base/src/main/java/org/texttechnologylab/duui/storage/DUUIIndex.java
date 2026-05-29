package org.texttechnologylab.duui.storage;

import org.texttechnologylab.duui.ems.DUUIResource;

import java.util.List;

public interface DUUIIndex<K, V> extends DUUIResource {
    Entry<K, V> add(K key, V value);

    boolean remove(K key, V value);

    List<V> remove(K key);

    List<V> find(K key);

    List<Entry<K, V>> entries();

    boolean contains(K key);

    long size();

    void clear();

    record Entry<K, V>(K key, V value) {
    }
}
