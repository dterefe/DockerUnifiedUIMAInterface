package org.texttechnologylab.duui.refactor.events;

import java.time.Instant;
import java.util.UUID;

public abstract sealed class DUUIEvent permits DUUILog, DUUIMetric {
    private final String id;
    private final Instant timestamp;
    private final DUUIEventType type;
    private final DUUIEventContext context;

    protected DUUIEvent(DUUIEventType type, DUUIEventContext context) {
        this.id = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
        this.type = type;
        this.context = context == null ? DUUIEventContext.current() : context;
    }

    public String id() {
        return id;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public DUUIEventType type() {
        return type;
    }

    public DUUIEventContext context() {
        return context;
    }
}
