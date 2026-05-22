package org.texttechnologylab.duui.storage;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUIStoragePrimitivesTest {
    @Test
    void registryStoresRemovesAndReturnsImmutableSnapshots() {
        DUUIRegistry<String, Proxy> registry = new DUUIInMemoryRegistry<>();
        registry.put("image:one", new Proxy("image:one"));

        assertEquals("image:one", registry.require("image:one").id());
        assertTrue(registry.contains("image:one"));
        assertThrows(UnsupportedOperationException.class, () -> registry.values().add(new Proxy("nope")));
        assertThrows(UnsupportedOperationException.class, () -> registry.keys().add("nope"));

        assertEquals("image:one", registry.remove("image:one").orElseThrow().id());
        assertFalse(registry.contains("image:one"));
    }

    @Test
    void cacheLoadsOnceInvalidatesAndExposesPolicy() {
        DUUICache.Policy policy = new DUUICache.Policy(10L, Duration.ofMinutes(1), null);
        DUUICache<String, String> cache = new DUUIInMemoryCache<>(policy);
        AtomicInteger loads = new AtomicInteger();

        String first = cache.getOrLoad("key", key -> {
            loads.incrementAndGet();
            return "value-" + key;
        });
        String second = cache.getOrLoad("key", key -> {
            loads.incrementAndGet();
            return "other-" + key;
        });

        assertEquals("value-key", first);
        assertEquals("value-key", second);
        assertEquals(1, loads.get());
        assertEquals(policy, cache.policy());
        assertEquals(1, cache.size());

        cache.invalidate("key");
        assertTrue(cache.get("key").isEmpty());

        DUUICache<String, String> bounded = new DUUIInMemoryCache<>(DUUICache.Policy.maximumSize(1));
        bounded.put("one", "1");
        bounded.put("two", "2");
        assertTrue(bounded.size() <= 1);
    }

    @Test
    void indexSupportsMultiValueLookupAndImmutableReads() {
        DUUIIndex<String, String> index = new DUUIInMemoryIndex<>();
        index.add("component", "container-1");
        index.add("component", "container-2");
        index.add("other", "container-3");

        assertEquals(List.of("container-1", "container-2"), index.find("component"));
        assertThrows(UnsupportedOperationException.class, () -> index.find("component").add("container-4"));

        assertTrue(index.remove("component", "container-1"));
        assertEquals(List.of("container-2"), index.find("component"));
        assertEquals(List.of("container-2"), index.remove("component"));
        assertFalse(index.contains("component"));
    }

    @Test
    void databaseQueryFiltersOrdersOffsetsLimitsAndCounts() {
        DUUIDatabase<String, Record> database = new DUUIInMemoryDatabase<>();
        database.put("a", new Record("a", "docker", 3));
        database.put("b", new Record("b", "docker", 1));
        database.put("c", new Record("c", "remote", 2));

        List<String> ids = database.query()
                .where(record -> record.kind().equals("docker"))
                .orderBy(Comparator.comparingInt(Record::rank))
                .offset(0)
                .limit(1)
                .list()
                .stream()
                .map(entry -> entry.value().id())
                .toList();

        assertEquals(List.of("b"), ids);
        assertEquals(2, database.query().where(record -> record.kind().equals("docker")).count());
        assertEquals("c", database.query().where(record -> record.rank() == 2).first().orElseThrow().key());
    }

    @Test
    void databaseViewsStayTypedForProxyLikeObjects() {
        DUUIInMemoryDatabase<String, Proxy> database = new DUUIInMemoryDatabase<>();
        database.put("image:one", new Proxy("image:one"));
        database.put("container:one", new Proxy("container:one"));

        DUUIRegistry<String, Proxy> registry = database.registry();
        DUUICache<String, Proxy> cache = database.cache();
        DUUIIndex<String, String> index = database.index("prefix", String.class, proxy -> proxy.id().split(":")[0]);

        assertEquals("image:one", registry.require("image:one").id());
        assertEquals("container:one", cache.getOrLoad("container:one", registry::require).id());
        assertEquals(List.of("image:one"), index.find("image"));
        assertEquals(List.of("container:one"), index.find("container"));
        assertThrows(IllegalArgumentException.class, () -> database.index("prefix", Integer.class));
    }

    @Test
    void databaseIndexesFollowRecordReplacement() {
        DUUIInMemoryDatabase<String, Record> database = new DUUIInMemoryDatabase<>();
        database.put("a", new Record("a", "old", 1));

        DUUIIndex<String, String> index = database.index("kind", String.class, Record::kind);
        database.put("a", new Record("a", "new", 1));

        assertEquals(List.of(), index.find("old"));
        assertEquals(List.of("a"), index.find("new"));
    }

    private record Proxy(String id) {
    }

    private record Record(String id, String kind, int rank) {
    }
}
