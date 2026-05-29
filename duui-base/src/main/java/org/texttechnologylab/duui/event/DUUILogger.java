package org.texttechnologylab.duui.event;

public final class DUUILogger {
    private final String name;
    private final DUUIEventService service;

    DUUILogger(String name, DUUIEventService service) {
        this.name = name;
        this.service = service;
    }

    public void trace(String message) { log(DUUIEventLevel.TRACE, message); }
    public void debug(String message) { log(DUUIEventLevel.DEBUG, message); }
    public void info(String message) { log(DUUIEventLevel.INFO, message); }
    public void warn(String message) { log(DUUIEventLevel.WARN, message); }
    public void warning(String message) { log(DUUIEventLevel.WARNING, message); }
    public void critical(String message) { log(DUUIEventLevel.CRITICAL, message); }

    public void error(String message) {
        service.emit(DUUIEvent.builder(DUUIEventType.ERROR)
                .context(service.currentContext())
                .name(name)
                .level(DUUIEventLevel.ERROR)
                .message(message)
                .build());
    }

    public void error(String message, Throwable error) {
        service.error(name, message, error, service.currentContext());
    }

    private void log(DUUIEventLevel level, String message) {
        service.emit(DUUIEvent.builder(DUUIEventType.LOG)
                .context(service.currentContext())
                .name(name)
                .level(level)
                .message(message)
                .build());
    }

    public void count(String metric) {
        count(metric, 1.0);
    }

    public void count(String metric, double value) {
        service.metric("processing", metric, value, "count", 0L, java.util.Map.of());
    }

    public void gauge(String metric, double value) {
        gauge(metric, value, "value");
    }

    public void gauge(String metric, double value, String unit) {
        service.metric("processing", metric, value, unit, 0L, java.util.Map.of());
    }

    public void timing(String metric, java.time.Duration elapsed) {
        service.metric("processing", metric, elapsed.toMillis(), "milliseconds", elapsed.toMillis(), java.util.Map.of());
    }
}
