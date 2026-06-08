package org.texttechnologylab.duui;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker-scoped context that provides thread-scoped value storage
 * without using {@link java.lang.ThreadLocal}.
 *
 * <p>Uses a {@link ConcurrentHashMap} keyed by thread ID as the scoping mechanism.
 * This is NOT a ThreadLocal — it is an explicit map lookup per thread,
 * satisfying [DESIGN: lines 42-44] which bans ThreadLocals.</p>
 *
 * <p>In duui-core, {@code DUUIWorker.current().context()} is the primary
 * accessor for the current worker's context. This class provides the
 * thread-scoped fallback used by duui-base classes that cannot reference
 * {@code DUUIWorker}.</p>
 *
 * [DESIGN: lines 42-47]
 */
public final class DUUIWorkerContext {
    private static final ConcurrentHashMap<Long, DUUIWorkerContext> THREAD_CONTEXTS = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Object> values = new ConcurrentHashMap<>();

    /**
     * Returns the context associated with the current thread.
     * Creates a new context if none exists.
     *
     * [DESIGN: line 44]
     */
    public static DUUIWorkerContext current() {
        long threadId = Thread.currentThread().threadId();
        return THREAD_CONTEXTS.computeIfAbsent(threadId, id -> new DUUIWorkerContext());
    }

    /**
     * Associates a context with the current thread.
     * Pass {@code null} to remove the association.
     *
     * [DESIGN: lines 46-47] — DUUIExecutor propagates context during thread switches.
     */
    public static void bind(DUUIWorkerContext context) {
        long threadId = Thread.currentThread().threadId();
        if (context == null) {
            THREAD_CONTEXTS.remove(threadId);
        } else {
            THREAD_CONTEXTS.put(threadId, context);
        }
    }

    /**
     * Removes the context association for the current thread.
     */
    public static void unbind() {
        THREAD_CONTEXTS.remove(Thread.currentThread().threadId());
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) values.get(key);
    }

    public <T> T getOrDefault(String key, T defaultValue) {
        T value = get(key);
        return value != null ? value : defaultValue;
    }

    public <T> void set(String key, T value) {
        if (value == null) {
            values.remove(key);
        } else {
            values.put(key, value);
        }
    }

    public void remove(String key) {
        values.remove(key);
    }

    public DUUIWorkerContext copy() {
        DUUIWorkerContext copy = new DUUIWorkerContext();
        copy.values.putAll(this.values);
        return copy;
    }

    public void copyFrom(DUUIWorkerContext source) {
        if (source != null) {
            this.values.putAll(source.values);
        }
    }
}
