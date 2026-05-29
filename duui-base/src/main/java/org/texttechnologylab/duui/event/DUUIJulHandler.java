package org.texttechnologylab.duui.event;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public final class DUUIJulHandler extends Handler {
    private final DUUIEventService service;

    public DUUIJulHandler(DUUIEventService service) {
        this.service = service == null ? DUUIEventService.global() : service;
    }

    @Override
    public void publish(LogRecord record) {
        if (record == null || !isLoggable(record)) return;
        DUUIEventLevel level = level(record.getLevel());
        DUUIEventType type = level == DUUIEventLevel.ERROR ? DUUIEventType.ERROR : DUUIEventType.LOG;
        DUUIEvent.Builder builder = DUUIEvent.builder(type)
                .context(service.currentContext())
                .name(record.getLoggerName())
                .level(level)
                .message(record.getMessage());
        if (record.getThrown() != null) {
            builder.error(record.getThrown().getClass().getName(), DUUIEventService.stackTrace(record.getThrown()), null);
        }
        service.emit(builder.build());
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }

    private static DUUIEventLevel level(Level level) {
        if (level == null) return DUUIEventLevel.INFO;
        if (level.intValue() >= Level.SEVERE.intValue()) return DUUIEventLevel.ERROR;
        if (level.intValue() >= Level.WARNING.intValue()) return DUUIEventLevel.WARN;
        if (level.intValue() <= Level.FINE.intValue()) return DUUIEventLevel.DEBUG;
        return DUUIEventLevel.INFO;
    }
}
