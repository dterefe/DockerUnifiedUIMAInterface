package org.texttechnologylab.duui.event;

public final class DUUILogger {
    private final String name;
    private final DUUIEventService service;

    DUUILogger(String name, DUUIEventService service) {
        this.name = name;
        this.service = service;
    }

    public void debug(String message) { log(DUUIEventLevel.DEBUG, message); }
    public void info(String message) { log(DUUIEventLevel.INFO, message); }
    public void warn(String message) { log(DUUIEventLevel.WARN, message); }

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
}
