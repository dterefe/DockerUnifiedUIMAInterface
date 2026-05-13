package org.texttechnologylab.duui.orchestration;

public final class DUUITaskScope implements AutoCloseable {
    private final DUUIWorker worker;
    private final DUUIWorker.DUITaskBinding binding;
    private boolean closed;

    DUUITaskScope(DUUITask<?> task) {
        this.worker = DUUIWorker.current();
        this.binding = new DUUIWorker.DUITaskBinding(task);
        this.worker.bind(binding);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        worker.clear(binding);
    }
}
