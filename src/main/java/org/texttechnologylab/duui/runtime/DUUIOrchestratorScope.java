package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.governance.DUUIGovernor;
import org.texttechnologylab.duui.orchestration.DUUIOrchestratorConfig;

public final class DUUIOrchestratorScope implements AutoCloseable {
    private final DUUISystemScope system;
    private boolean failFast;
    private boolean stopOnUnroutableArtifact = true;
    private DUUIGovernor governor = DUUIGovernor.NONE;
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

    public DUUIOrchestratorScope governor(DUUIGovernor governor) {
        this.governor = governor == null ? DUUIGovernor.NONE : governor;
        return this;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        system.orchestratorConfig(new DUUIOrchestratorConfig(failFast, stopOnUnroutableArtifact, governor));
    }
}
