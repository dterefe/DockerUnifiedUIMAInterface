package org.texttechnologylab.duui.refactor.events;

import java.util.Map;
import java.util.Objects;

public final class DUUILog extends DUUIEvent {
    private final DUUILogLevel level;
    private final String message;
    private final Map<String, String> values;

    public DUUILog(DUUILogLevel level, String message) {
        this(level, message, Map.of(), DUUIEventContext.current());
    }

    public DUUILog(DUUILogLevel level, String message, Map<String, String> values, DUUIEventContext context) {
        super(DUUIEventType.LOG, context);
        this.level = Objects.requireNonNull(level, "level");
        this.message = Objects.requireNonNull(message, "message");
        this.values = Map.copyOf(values == null ? Map.of() : values);
    }

    public DUUILogLevel level() {
        return level;
    }

    public String message() {
        return message;
    }

    public Map<String, String> values() {
        return values;
    }
}
