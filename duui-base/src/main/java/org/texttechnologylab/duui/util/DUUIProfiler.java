package org.texttechnologylab.duui.util;

import org.texttechnologylab.duui.event.DUUIEventLevel;
import org.texttechnologylab.duui.event.DUUIEventService;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A lightweight profiling framework that wraps around processing phases and emits timing
 * metrics via {@link DUUIEventService}. Shared by both duui-core v1 (composer) and v2
 * (orchestrator) — no duplication.
 *
 * <h3>Programmatic usage</h3>
 * <pre>{@code
 * try (DUUIProfiler.Span span = DUUIProfiler.start("serialize")) {
 *     // serialize work
 * } // auto-closes, emits metric
 * }</pre>
 *
 * <h3>Declarative usage</h3>
 * <p>Annotate methods with {@link DUUITimed @DUUITimed}. When a method interceptor or
 * wrapper detects the annotation, it wraps the call in a {@link Span}.</p>
 *
 * <h3>Toggle</h3>
 * <p>Profiling is enabled by default but can be disabled by setting the system property
 * {@code duui.profiler.enabled=false} or the environment variable {@code DUUI_PROFILER_ENABLED=false}.
 * When disabled, {@link #start(String)} returns a no-op span.</p>
 *
 * <p>Phases tracked: {@code serialize}, {@code process} (annotator), {@code deserialize},
 * and arbitrary custom phases.</p>
 */
public final class DUUIProfiler {

    private static final String PROP_ENABLED = "duui.profiler.enabled";
    private static final String ENV_ENABLED = "DUUI_PROFILER_ENABLED";

    private static final AtomicBoolean ENABLED = new AtomicBoolean(resolveEnabled());
    private static final Map<String, Long> ACTIVE_SPANS = new ConcurrentHashMap<>();

    private DUUIProfiler() {
        // utility class
    }

    /**
     * Returns {@code true} if profiling is currently enabled.
     */
    public static boolean enabled() {
        return ENABLED.get();
    }

    /**
     * Programmatically enable or disable profiling at runtime.
     */
    public static void enabled(boolean enabled) {
        ENABLED.set(enabled);
    }

    /**
     * Starts a new profiling span with the given phase name.
     * When the span is closed, a timing metric is emitted to {@link DUUIEventService#global()}.
     *
     * @param phase the phase name (e.g., "serialize", "process", "deserialize")
     * @return a {@link Span} that records timing; a no-op span if profiling is disabled
     */
    public static Span start(String phase) {
        return start(phase, DUUIEventService.global());
    }

    /**
     * Starts a new profiling span that emits metrics to the given event service.
     *
     * @param phase  the phase name
     * @param events the event service to emit metrics to
     * @return a {@link Span} that records timing; a no-op span if profiling is disabled
     */
    public static Span start(String phase, DUUIEventService events) {
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(events, "events");
        if (!ENABLED.get()) {
            return NoOpSpan.INSTANCE;
        }
        return new ActiveSpan(phase, events, Instant.now());
    }

    /**
     * Returns the current number of active spans. Useful for debugging / leak detection.
     */
    public static int activeSpanCount() {
        return ACTIVE_SPANS.size();
    }

    // ── Span interface ─────────────────────────────────────────────────

    /**
     * A named, auto-closeable timing span. Close it to emit a duration metric.
     */
    public interface Span extends AutoCloseable {
        /**
         * The phase name this span tracks.
         */
        String phase();

        /**
         * Elapsed time since the span started, or {@link Duration#ZERO} if already closed.
         */
        Duration elapsed();

        /**
         * Closes the span and emits a {@code METRIC} event with the elapsed duration.
         */
        @Override
        void close();
    }

    // ── Active span ────────────────────────────────────────────────────

    private static final class ActiveSpan implements Span {
        private final String phase;
        private final DUUIEventService events;
        private final Instant start;
        private volatile boolean closed;
        private Duration elapsed;

        ActiveSpan(String phase, DUUIEventService events, Instant start) {
            this.phase = phase;
            this.events = events;
            this.start = start;
            ACTIVE_SPANS.put(phase + "@" + System.identityHashCode(this), Thread.currentThread().threadId());
        }

        @Override
        public String phase() {
            return phase;
        }

        @Override
        public Duration elapsed() {
            if (elapsed != null) {
                return elapsed;
            }
            return Duration.between(start, Instant.now());
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            elapsed = Duration.between(start, Instant.now());
            ACTIVE_SPANS.remove(phase + "@" + System.identityHashCode(this));

            long millis = elapsed.toMillis();
            events.metric(
                    "duui.profiler." + phase,
                    "duration_ms",
                    (double) millis,
                    "ms",
                    millis,
                    Map.of("phase", phase)
            );
        }
    }

    // ── No-op span ─────────────────────────────────────────────────────

    private static final class NoOpSpan implements Span {
        static final NoOpSpan INSTANCE = new NoOpSpan();

        @Override
        public String phase() {
            return "noop";
        }

        @Override
        public Duration elapsed() {
            return Duration.ZERO;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    // ── Enable/disable resolution ──────────────────────────────────────

    private static boolean resolveEnabled() {
        String prop = System.getProperty(PROP_ENABLED);
        if (prop != null && !prop.isBlank()) {
            return Boolean.parseBoolean(prop.trim());
        }
        String env = System.getenv(ENV_ENABLED);
        if (env != null && !env.isBlank()) {
            return Boolean.parseBoolean(env.trim());
        }
        return true; // enabled by default
    }
}
