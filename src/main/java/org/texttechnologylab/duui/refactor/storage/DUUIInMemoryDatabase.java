package org.texttechnologylab.duui.refactor.storage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class DUUIInMemoryDatabase<K, R> implements DUUIDatabase<K, R> {
    private final DUUIInMemoryRegistry<K, R> registry;
    private final DUUIInMemoryCache<K, R> cache;
    private final Cache<String, TypedIndex<K, R, ?>> indexes;

    public DUUIInMemoryDatabase() {
        this(new DUUIInMemoryRegistry<>(), new DUUIInMemoryCache<>());
    }

    public DUUIInMemoryDatabase(DUUIInMemoryRegistry<K, R> registry, DUUIInMemoryCache<K, R> cache) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.indexes = Caffeine.newBuilder().build();
    }

    @Override
    public DUUIRegistry.Entry<K, R> put(K key, R record) {
        Optional<R> previous = registry.get(key);
        DUUIRegistry.Entry<K, R> entry = registry.put(key, record);
        cache.put(key, record);
        previous.ifPresent(oldRecord -> indexes.asMap().values().forEach(index -> index.remove(key, oldRecord)));
        indexes.asMap().values().forEach(index -> index.add(key, record));
        return entry;
    }

    @Override
    public Optional<R> get(K key) {
        Optional<R> cached = cache.get(key);
        if (cached.isPresent()) {
            return cached;
        }
        Optional<R> value = registry.get(key);
        value.ifPresent(record -> cache.put(key, record));
        return value;
    }

    @Override
    public R require(K key) {
        return get(key).orElseThrow(() -> new java.util.NoSuchElementException("No DUUI database record for key " + key));
    }

    @Override
    public Optional<R> delete(K key) {
        Optional<R> removed = registry.remove(key);
        cache.invalidate(key);
        removed.ifPresent(record -> indexes.asMap().values().forEach(index -> index.remove(key, record)));
        return removed;
    }

    @Override
    public Query<K, R> query() {
        return new QueryView<>(registry.entries(), List.of(), null, -1L, 0L);
    }

    @Override
    public DUUIRegistry<K, R> registry() {
        return registry;
    }

    @Override
    public DUUICache<K, R> cache() {
        return cache;
    }

    @Override
    public <I> DUUIIndex<I, K> index(String name, Class<I> keyType) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(keyType, "keyType");
        TypedIndex<K, R, ?> existing = indexes.getIfPresent(name);
        if (existing == null) {
            TypedIndex<K, R, I> created = new TypedIndex<>(keyType, null);
            indexes.put(name, created);
            return created.index();
        }
        return existing.cast(keyType);
    }

    @Override
    public <I> DUUIIndex<I, K> index(String name, Class<I> keyType, Function<R, I> extractor) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(keyType, "keyType");
        Objects.requireNonNull(extractor, "extractor");
        DUUIIndex<I, K> index = new DUUIInMemoryIndex<>();
        for (DUUIRegistry.Entry<K, R> entry : registry.entries()) {
            I indexKey = extractor.apply(entry.value());
            if (indexKey != null) {
                index.add(indexKey, entry.key());
            }
        }
        indexes.put(name, new TypedIndex<>(keyType, extractor, index));
        return index;
    }

    private record QueryView<K, R>(
            List<DUUIRegistry.Entry<K, R>> source,
            List<Predicate<R>> predicates,
            Comparator<R> comparator,
            long limit,
            long offset
    ) implements Query<K, R> {
        @Override
        public Query<K, R> where(Predicate<R> predicate) {
            Objects.requireNonNull(predicate, "predicate");
            List<Predicate<R>> next = new ArrayList<>(predicates);
            next.add(predicate);
            return new QueryView<>(source, List.copyOf(next), comparator, limit, offset);
        }

        @Override
        public Query<K, R> orderBy(Comparator<R> comparator) {
            return new QueryView<>(source, predicates, Objects.requireNonNull(comparator, "comparator"), limit, offset);
        }

        @Override
        public Query<K, R> limit(long limit) {
            return new QueryView<>(source, predicates, comparator, Math.max(0L, limit), offset);
        }

        @Override
        public Query<K, R> offset(long offset) {
            return new QueryView<>(source, predicates, comparator, limit, Math.max(0L, offset));
        }

        @Override
        public Stream<DUUIRegistry.Entry<K, R>> stream() {
            Stream<DUUIRegistry.Entry<K, R>> stream = source.stream();
            for (Predicate<R> predicate : predicates) {
                stream = stream.filter(entry -> predicate.test(entry.value()));
            }
            if (comparator != null) {
                stream = stream.sorted((left, right) -> comparator.compare(left.value(), right.value()));
            }
            if (offset > 0L) {
                stream = stream.skip(offset);
            }
            if (limit >= 0L) {
                stream = stream.limit(limit);
            }
            return stream;
        }

        @Override
        public List<DUUIRegistry.Entry<K, R>> list() {
            return stream().toList();
        }

        @Override
        public Optional<DUUIRegistry.Entry<K, R>> first() {
            return stream().findFirst();
        }

        @Override
        public long count() {
            return stream().count();
        }
    }

    private static final class TypedIndex<K, R, I> {
        private final Class<I> keyType;
        private final Function<R, I> extractor;
        private final DUUIIndex<I, K> index;

        private TypedIndex(Class<I> keyType, Function<R, I> extractor) {
            this(keyType, extractor, new DUUIInMemoryIndex<>());
        }

        private TypedIndex(Class<I> keyType, Function<R, I> extractor, DUUIIndex<I, K> index) {
            this.keyType = Objects.requireNonNull(keyType, "keyType");
            this.extractor = extractor;
            this.index = Objects.requireNonNull(index, "index");
        }

        private void add(K key, R record) {
            if (extractor == null) {
                return;
            }
            I indexKey = extractor.apply(record);
            if (indexKey != null) {
                index.add(indexKey, key);
            }
        }

        private void remove(K key, R record) {
            if (extractor == null) {
                return;
            }
            I indexKey = extractor.apply(record);
            if (indexKey != null) {
                index.remove(indexKey, key);
            }
        }

        private <T> DUUIIndex<T, K> cast(Class<T> requestedType) {
            if (!keyType.equals(requestedType)) {
                throw new IllegalArgumentException("DUUI index key type mismatch. Existing="
                        + keyType.getName() + ", requested=" + requestedType.getName());
            }
            @SuppressWarnings("unchecked")
            DUUIIndex<T, K> typed = (DUUIIndex<T, K>) index;
            return typed;
        }

        private DUUIIndex<I, K> index() {
            return index;
        }
    }
}
