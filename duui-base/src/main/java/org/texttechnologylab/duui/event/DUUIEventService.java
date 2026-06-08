package org.texttechnologylab.duui.event;

import org.texttechnologylab.duui.DUUIWorkerContext;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

public final class DUUIEventService implements AutoCloseable {
    private static final String KEY_SERVICE = "event.service";
    private static final String KEY_CONTEXT = "event.context";

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
        DUUIWorkerContext ctx = DUUIWorkerContext.current();
        DUUIEventService service = ctx.get(KEY_SERVICE);
        return service == null ? GLOBAL : service;
    }

    public static void bindCurrent(DUUIEventService service, DUUIEventContext context) {
        DUUIWorkerContext ctx = DUUIWorkerContext.current();
        if (service == null) {
            ctx.remove(KEY_SERVICE);
        } else {
            ctx.set(KEY_SERVICE, service);
        }
        bindCurrentContext(context);
    }

    public static void bindCurrentContext(DUUIEventContext context) {
        DUUIWorkerContext ctx = DUUIWorkerContext.current();
        if (context == null) {
            ctx.remove(KEY_CONTEXT);
        } else {
            ctx.set(KEY_CONTEXT, context);
        }
    }

    public static void clearCurrent() {
        DUUIWorkerContext ctx = DUUIWorkerContext.current();
        ctx.remove(KEY_SERVICE);
        ctx.remove(KEY_CONTEXT);
    }

    public static void runWithCurrent(DUUIEventService service, DUUIEventContext context, Runnable work) {
        try {
            callWithCurrent(service, context, () -> {
                work.run();
                return null;
            });
        } catch (RuntimeException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }

    public static <T> T callWithCurrent(DUUIEventService service, DUUIEventContext context, Callable<T> work) throws Exception {
        DUUIWorkerContext ctx = DUUIWorkerContext.current();
        DUUIEventService previousService = ctx.get(KEY_SERVICE);
        DUUIEventContext previousContext = ctx.get(KEY_CONTEXT);
        bindCurrent(service, context);
        try {
            return work.call();
        } finally {
            bindCurrent(previousService, previousContext);
        }
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
        emit(new DUUILog(level, message, java.util.Map.of()).event(name, currentContext()));
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
        DUUIWorkerContext ctx = DUUIWorkerContext.current();
        DUUIEventContext context = ctx.get(KEY_CONTEXT);
        DUUIEventContext.Builder builder = context == null ? new DUUIEventContext.Builder() : context.toBuilder();
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
