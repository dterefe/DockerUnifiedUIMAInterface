package org.texttechnologylab.duui.event;

import java.util.LinkedHashMap;
import java.util.Map;

public final class DUUILogger {
    private static final String DEFAULT_NAME = "duui";

    private final String name;
    private final DUUIEventService service;

    DUUILogger(String name, DUUIEventService service) {
        this.name = name;
        this.service = service;
    }

    public static DUUILogger get() {
        return get(DEFAULT_NAME);
    }

    public static DUUILogger get(String name) {
        return DUUIEventService.current().logger(name == null || name.isBlank() ? DEFAULT_NAME : name);
    }

    public void trace(String message, String... args) { log(DUUIEventLevel.TRACE, message, args); }
    public void debug(String message, String... args) { log(DUUIEventLevel.DEBUG, message, args); }
    public void info(String message, String... args) { log(DUUIEventLevel.INFO, message, args); }
    public void warn(String message, String... args) { log(DUUIEventLevel.WARN, message, args); }
    public void warning(String message, String... args) { log(DUUIEventLevel.WARNING, message, args); }
    public void error(String message, String... args) { log(DUUIEventLevel.ERROR, message, args); }
    public void critical(String message, String... args) { log(DUUIEventLevel.CRITICAL, message, args); }
    public void fatal(String message, String... args) { log(DUUIEventLevel.CRITICAL, message, args); }
    public void severe(String message, String... args) { log(DUUIEventLevel.ERROR, message, args); }

    public void error(String message, Throwable error) {
        service.emit(new DUUILog(DUUIEventLevel.ERROR, message, Map.of())
                .event(name, service.currentContext())
                .toBuilder()
                .error(error == null ? null : error.getClass().getName(), DUUIEventService.stackTrace(error), null)
                .build());
    }

    public void error(String message, Throwable error, String... args) {
        service.emit(new DUUILog(DUUIEventLevel.ERROR, message, attributes(args))
                .event(name, service.currentContext())
                .toBuilder()
                .error(error == null ? null : error.getClass().getName(), DUUIEventService.stackTrace(error), null)
                .build());
    }

    private void log(DUUIEventLevel level, String message, String... args) {
        service.emit(new DUUILog(level, message, attributes(args)).event(name, service.currentContext()));
    }

    private static Map<String, Object> attributes(String... args) {
        if (args == null || args.length == 0) {
            return Map.of();
        }
        Map<String, Object> attributes = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg == null) {
                continue;
            }
            int separator = arg.indexOf('=');
            if (separator > 0) {
                attributes.put(arg.substring(0, separator), arg.substring(separator + 1));
            } else {
                attributes.put("arg" + i, arg);
            }
        }
        return attributes;
    }
}
