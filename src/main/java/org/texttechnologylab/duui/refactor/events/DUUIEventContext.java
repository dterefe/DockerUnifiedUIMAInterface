package org.texttechnologylab.duui.refactor.events;

import org.texttechnologylab.duui.refactor.timelines.DUUIPhase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public record DUUIEventContext(
        String phase,
        String status,
        String lifecycle,
        String thread,
        Map<String, String> values
) {
    private static final ThreadLocal<DUUIPhase> PHASE = new ThreadLocal<>();
    private static final ThreadLocal<Map<String, String>> VALUES = ThreadLocal.withInitial(LinkedHashMap::new);

    public DUUIEventContext {
        values = Map.copyOf(values == null ? Map.of() : values);
    }

    public static void phase(DUUIPhase phase) {
        if (phase == null) {
            PHASE.remove();
            return;
        }
        PHASE.set(phase);
    }

    public static Optional<DUUIPhase> currentPhase() {
        return Optional.ofNullable(PHASE.get());
    }

    public static void put(String key, String value) {
        if (value == null) {
            VALUES.get().remove(key);
            return;
        }
        VALUES.get().put(key, value);
    }

    public static void put(Map<String, String> values) {
        if (values != null) {
            values.forEach(DUUIEventContext::put);
        }
    }

    public static void clear() {
        PHASE.remove();
        VALUES.remove();
    }

    public static DUUIEventContext current() {
        DUUIPhase phase = PHASE.get();
        return new DUUIEventContext(
                phase == null ? null : phase.id(),
                phase == null ? null : phase.status().name(),
                phase == null ? null : phase.lifecycle().name(),
                Thread.currentThread().getName(),
                VALUES.get()
        );
    }

    public Map<String, String> flattened() {
        LinkedHashMap<String, String> context = new LinkedHashMap<>(values);
        if (phase != null) {
            context.put("phase", phase);
        }
        if (status != null) {
            context.put("status", status);
        }
        if (lifecycle != null) {
            context.put("lifecycle", lifecycle);
        }
        context.put("thread", thread);
        return Map.copyOf(context);
    }
}
