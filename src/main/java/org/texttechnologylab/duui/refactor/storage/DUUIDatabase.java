package org.texttechnologylab.duui.refactor.storage;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface DUUIDatabase<K, R> {
    DUUIRegistry.Entry<K, R> put(K key, R record);

    Optional<R> get(K key);

    R require(K key);

    Optional<R> delete(K key);

    Query<K, R> query();

    DUUIRegistry<K, R> registry();

    DUUICache<K, R> cache();

    <I> DUUIIndex<I, K> index(String name, Class<I> keyType);

    <I> DUUIIndex<I, K> index(String name, Class<I> keyType, Function<R, I> extractor);

    interface Query<K, R> {
        Query<K, R> where(Predicate<R> predicate);

        Query<K, R> orderBy(Comparator<R> comparator);

        Query<K, R> limit(long limit);

        Query<K, R> offset(long offset);

        Stream<DUUIRegistry.Entry<K, R>> stream();

        List<DUUIRegistry.Entry<K, R>> list();

        Optional<DUUIRegistry.Entry<K, R>> first();

        long count();
    }
}
