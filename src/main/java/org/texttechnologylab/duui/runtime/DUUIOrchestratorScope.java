package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.orchestration.DUUIOrchestratorConfig;

public final class DUUIOrchestratorScope implements AutoCloseable {
    private final DUUISystemScope system;
    private boolean failFast;
    private boolean stopOnUnroutableArtifact = true;
    private boolean closed;

    DUUIOrchestratorScope(DUUISystemScope system) {
        this.system = system;
    }

    public DUUIOrchestratorScope failFast(boolean failFast) {
        this.failFast = failFast;
        return this;
    }

    public DUUIOrchestratorScope stopOnUnroutableArtifact(boolean stopOnUnroutableArtifact) {
        this.stopOnUnroutableArtifact = stopOnUnroutableArtifact;
        return this;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        system.orchestratorConfig(new DUUIOrchestratorConfig(failFast, stopOnUnroutableArtifact));
    }
}
