package org.texttechnologylab.duui.artifact;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class DUUIStats {
    private final Map<String, AtomicLong> counters = new LinkedHashMap<>();

    public void increment(String key) { add(key, 1); }
    public void add(String key, long value) { counters.computeIfAbsent(key, ignored -> new AtomicLong()).addAndGet(value); }
    public long get(String key) { return counters.getOrDefault(key, new AtomicLong()).get(); }

    public Map<String, Long> snapshot() {
        Map<String, Long> snapshot = new LinkedHashMap<>();
        counters.forEach((key, value) -> snapshot.put(key, value.get()));
        return Collections.unmodifiableMap(snapshot);
    }
}
