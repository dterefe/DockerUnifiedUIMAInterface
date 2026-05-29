package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.governance.DUUIGovernor;

import java.util.Objects;

public final class DUUIOrchestratorConfig {
    public static final DUUIOrchestratorConfig DEFAULT = new DUUIOrchestratorConfig(false, true);

    private final boolean failFast;
    private final boolean stopOnUnroutableArtifact;
    private final DUUIGovernor governor;

    public DUUIOrchestratorConfig(boolean failFast, boolean stopOnUnroutableArtifact) {
        this(failFast, stopOnUnroutableArtifact, DUUIGovernor.NONE);
    }

    public DUUIOrchestratorConfig(boolean failFast, boolean stopOnUnroutableArtifact, DUUIGovernor governor) {
        this.failFast = failFast;
        this.stopOnUnroutableArtifact = stopOnUnroutableArtifact;
        this.governor = Objects.requireNonNullElse(governor, DUUIGovernor.NONE);
    }

    public boolean failFast() { return failFast; }
    public boolean stopOnUnroutableArtifact() { return stopOnUnroutableArtifact; }
    public DUUIGovernor governor() { return governor; }
}
