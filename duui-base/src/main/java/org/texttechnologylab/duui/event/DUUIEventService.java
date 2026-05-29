package org.texttechnologylab.duui.event;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

public final class DUUIEventService implements AutoCloseable {
    private static final DUUIEventService GLOBAL = new DUUIEventService(List.of(DUUIEventSinks.noOp()));

    private final CopyOnWriteArrayList<DUUIEventSink> sinks = new CopyOnWriteArrayList<>();

    public DUUIEventService(List<DUUIEventSink> sinks) {
        if (sinks != null) this.sinks.addAll(sinks);
        if (this.sinks.isEmpty()) this.sinks.add(DUUIEventSinks.noOp());
    }

    public static DUUIEventService global() {
        return GLOBAL;
    }

    public static DUUIEventService current() {
        return GLOBAL;
    }

    public void addSink(DUUIEventSink sink) {
        if (sink != null) sinks.add(sink);
    }

    public void setSinks(List<DUUIEventSink> sinks) {
        this.sinks.clear();
        if (sinks != null) this.sinks.addAll(sinks);
        if (this.sinks.isEmpty()) this.sinks.add(DUUIEventSinks.noOp());
    }

    public void emit(DUUIEvent event) {
        Objects.requireNonNull(event, "event");
        for (DUUIEventSink sink : sinks) {
            try {
                sink.accept(event);
            } catch (RuntimeException ignored) {
                // Event sinks must not control DUUI execution.
            }
        }
    }

    public DUUILogger logger(String name) {
        return new DUUILogger(name, this);
    }

    public DUUIEventScope scope(String name) {
        return new DUUIEventScope(this, name, currentContext().toBuilder().trace(currentContext().trace().child()).build());
    }

    public <T> T scoped(String name, Callable<T> work) throws Exception {
        try (DUUIEventScope scope = scope(name)) {
            try {
                return work.call();
            } catch (Exception error) {
                scope.fail(error);
                throw error;
            }
        }
    }

    public void log(String name, DUUIEventLevel level, String message) {
        emit(DUUIEvent.builder(DUUIEventType.LOG)
                .context(currentContext())
                .name(name)
                .level(level)
                .message(message)
                .build());
    }

    public void metric(String category, String name, double value, String unit, long intervalMs, java.util.Map<String, String> tags) {
        emit(DUUIEvent.builder(DUUIEventType.METRIC)
                .context(currentContext())
                .name(category)
                .metric(name, value, unit, intervalMs)
                .metricTags(tags)
                .build());
    }

    public void error(String name, Throwable error, DUUIEventContext context) {
        error(name, error == null ? null : error.getMessage(), error, context);
    }

    public void error(String name, String message, Throwable error, DUUIEventContext context) {
        emit(DUUIEvent.builder(DUUIEventType.ERROR)
                .context(context == null ? currentContext() : context)
                .name(name)
                .level(DUUIEventLevel.ERROR)
                .message(message)
                .error(error == null ? null : error.getClass().getName(), stackTrace(error), null)
                .build());
    }

    public DUUIEventContext currentContext() {
        DUUIEventContext.Builder builder = new DUUIEventContext.Builder();
        DUUIEventContext.currentPhaseId().ifPresent(builder::phaseId);
        DUUIEventContext.currentPhaseStatus().ifPresent(builder::phaseStatus);
        DUUIEventContext.currentPhaseLifecycle().ifPresent(builder::phaseLifecycle);
        return builder.build();
    }

    public static String stackTrace(Throwable error) {
        if (error == null) return null;
        StringWriter writer = new StringWriter();
        error.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    @Override
    public void close() {
        List<DUUIEventSink> copy = new ArrayList<>(sinks);
        for (DUUIEventSink sink : copy) {
            try {
                sink.close();
            } catch (Exception ignored) {
            }
        }
    }
}
