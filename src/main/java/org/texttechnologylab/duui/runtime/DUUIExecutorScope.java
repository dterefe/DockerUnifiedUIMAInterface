package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.exception.DUUIFailureClassifier;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;

public final class DUUIExecutorScope implements AutoCloseable {
    private final DUUISystemScope system;
    private DUUIFailureClassifier failureClassifier = new DUUIFailureClassifier();
    private boolean closed;

    DUUIExecutorScope(DUUISystemScope system) {
        this.system = system;
    }

    public DUUIExecutorScope failureClassifier(DUUIFailureClassifier failureClassifier) {
        this.failureClassifier = failureClassifier == null ? new DUUIFailureClassifier() : failureClassifier;
        return this;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        system.executor(new DUUIExecutor(system.id(), failureClassifier));
    }
}
