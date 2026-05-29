package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.orchestration.worker.DUUIWorker;

public final class DUUITaskScope implements AutoCloseable {
    private final DUUIWorker worker;
    private final DUUITask<?> task;
    private boolean closed;

    DUUITaskScope(DUUITask<?> task) {
        this.worker = DUUIWorker.current();
        this.task = task;
        this.worker.bind(task);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        worker.clear(task);
    }
}
