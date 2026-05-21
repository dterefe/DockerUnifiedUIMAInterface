package org.texttechnologylab.duui.refactor.events;

import org.texttechnologylab.duui.refactor.storage.DUUIInMemoryIndex;
import org.texttechnologylab.duui.refactor.storage.DUUIInMemoryRegistry;
import org.texttechnologylab.duui.refactor.storage.DUUIIndex;
import org.texttechnologylab.duui.refactor.storage.DUUIRegistry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

public final class DUUILogger {
    private final DUUIRegistry<String, DUUIEvent> events;
    private final DUUIIndex<DUUIEventType, String> types;
    private final DUUIIndex<DUUILogLevel, String> levels;
    private final DUUIIndex<String, String> phases;
    private final DUUIRegistry<String, Consumer<DUUIEvent>> sinks;

    public DUUILogger() {
        this(new DUUIInMemoryRegistry<>(), new DUUIInMemoryIndex<>(), new DUUIInMemoryIndex<>(), new DUUIInMemoryIndex<>(), new DUUIInMemoryRegistry<>());
    }

    public DUUILogger(
            DUUIRegistry<String, DUUIEvent> events,
            DUUIIndex<DUUIEventType, String> types,
            DUUIIndex<DUUILogLevel, String> levels,
            DUUIIndex<String, String> phases,
            DUUIRegistry<String, Consumer<DUUIEvent>> sinks
    ) {
        this.events = Objects.requireNonNull(events, "events");
        this.types = Objects.requireNonNull(types, "types");
        this.levels = Objects.requireNonNull(levels, "levels");
        this.phases = Objects.requireNonNull(phases, "phases");
        this.sinks = Objects.requireNonNull(sinks, "sinks");
    }

    public DUUIRegistry.Entry<String, Consumer<DUUIEvent>> sink(String name, Consumer<DUUIEvent> sink) {
        return sinks.put(name, sink);
    }

    public DUUILog debug(String message) {
        return log(DUUILogLevel.DEBUG, message);
    }

    public DUUILog info(String message) {
        return log(DUUILogLevel.INFO, message);
    }

    public DUUILog warning(String message) {
        return log(DUUILogLevel.WARNING, message);
    }

    public DUUILog error(String message) {
        return log(DUUILogLevel.ERROR, message);
    }

    public DUUILog critical(String message) {
        return log(DUUILogLevel.CRITICAL, message);
    }

    public DUUILog log(DUUILogLevel level, String message) {
        return emit(new DUUILog(level, message));
    }

    public DUUIMetric count(String name) {
        return count(name, 1.0);
    }

    public DUUIMetric count(String name, double value) {
        return metric("processing", name, value, "count", Duration.ZERO, Map.of());
    }

    public DUUIMetric gauge(String name, double value) {
        return gauge(name, value, "value");
    }

    public DUUIMetric gauge(String name, double value, String unit) {
        return metric("processing", name, value, unit, Duration.ZERO, Map.of());
    }

    public DUUIMetric timing(String name, Duration elapsed) {
        return metric("processing", name, elapsed.toMillis(), "milliseconds", elapsed, Map.of());
    }

    public Timer timer(String name) {
        return new Timer(this, name);
    }

    public DUUIMetric metric(String category, String name, double value, String unit, Duration interval, Map<String, String> tags) {
        return emit(new DUUIMetric(category, name, value, unit, interval, tags));
    }

    public <E extends DUUIEvent> E emit(E event) {
        events.put(event.id(), event);
        types.add(event.type(), event.id());
        if (event.context().phase() != null) {
            phases.add(event.context().phase(), event.id());
        }
        if (event instanceof DUUILog log) {
            levels.add(log.level(), log.id());
        }
        sinks.values().forEach(sink -> sink.accept(event));
        return event;
    }

    public Stream<DUUIEvent> events() {
        return events.values().stream();
    }

    public Stream<DUUIEvent> events(DUUIEventType type) {
        return types.find(type).stream().map(events::require);
    }

    public Stream<DUUILog> logs(DUUILogLevel level) {
        return levels.find(level).stream().map(events::require).map(DUUILog.class::cast);
    }

    public Stream<DUUIEvent> phase(String phase) {
        return phases.find(phase).stream().map(events::require);
    }

    public List<DUUIEvent> snapshot() {
        return events.values();
    }

    public static final class Timer implements AutoCloseable {
        private final DUUILogger logger;
        private final String name;
        private final long started;
        private boolean closed;

        private Timer(DUUILogger logger, String name) {
            this.logger = logger;
            this.name = name;
            this.started = System.nanoTime();
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            logger.timing(name, Duration.ofNanos(System.nanoTime() - started));
        }
    }
}
