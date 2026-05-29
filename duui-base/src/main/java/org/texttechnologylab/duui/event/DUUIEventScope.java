package org.texttechnologylab.duui.event;

import java.util.concurrent.atomic.AtomicBoolean;

public final class DUUIEventScope implements AutoCloseable {
    private final DUUIEventService service;
    private final String name;
    private final DUUIEventContext context;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    DUUIEventScope(DUUIEventService service, String name, DUUIEventContext context) {
        this.service = service;
        this.name = name;
        this.context = context;
        service.emit(DUUIEvent.builder(DUUIEventType.STATUS)
                .context(context)
                .name(name)
                .status(DUUIEventStatus.STARTED)
                .build());
    }

    public void fail(Throwable error) {
        if (!finished.compareAndSet(false, true)) return;
        service.emit(DUUIEvent.builder(DUUIEventType.STATUS)
                .context(context)
                .name(name)
                .status(DUUIEventStatus.FAILED)
                .message(error == null ? null : error.getMessage())
                .build());
        if (error != null) {
            service.error(name, error, context);
        }
    }

    @Override
    public void close() {
        if (!finished.compareAndSet(false, true)) return;
        service.emit(DUUIEvent.builder(DUUIEventType.STATUS)
                .context(context)
                .name(name)
                .status(DUUIEventStatus.COMPLETED)
                .build());
    }
}
