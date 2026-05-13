package org.texttechnologylab.duui.event;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class DUUIEventSinks {
    private DUUIEventSinks() {
    }

    public static DUUIEventSink noOp() {
        return ignored -> {
        };
    }

    public static DUUIEventSink console() {
        return event -> System.out.println(format(event));
    }

    public static DUUIEventSink jul(Logger logger) {
        Logger sinkLogger = logger == null ? Logger.getLogger("org.texttechnologylab.duui.events") : logger;
        return event -> sinkLogger.log(level(event), format(event));
    }

    private static Level level(DUUIEvent event) {
        if (event.type() == DUUIEventType.ERROR || event.level() == DUUIEventLevel.ERROR) return Level.SEVERE;
        if (event.level() == DUUIEventLevel.WARN) return Level.WARNING;
        if (event.level() == DUUIEventLevel.DEBUG || event.level() == DUUIEventLevel.TRACE) return Level.FINE;
        return Level.INFO;
    }

    private static String format(DUUIEvent event) {
        return "[" + event.type() + "]"
                + (event.status() == null ? "" : " " + event.status())
                + (event.level() == null ? "" : " " + event.level())
                + (event.name() == null ? "" : " " + event.name())
                + (event.message() == null ? "" : " - " + event.message())
                + " trace=" + event.traceId()
                + " task=" + event.taskId()
                + " artifact=" + event.artifactId();
    }
}
