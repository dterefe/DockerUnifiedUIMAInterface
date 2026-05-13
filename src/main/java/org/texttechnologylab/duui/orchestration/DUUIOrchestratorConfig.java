package org.texttechnologylab.duui.orchestration;

public final class DUUIOrchestratorConfig {
    public static final DUUIOrchestratorConfig DEFAULT = new DUUIOrchestratorConfig(false, true);

    private final boolean failFast;
    private final boolean stopOnUnroutableArtifact;

    public DUUIOrchestratorConfig(boolean failFast, boolean stopOnUnroutableArtifact) {
        this.failFast = failFast;
        this.stopOnUnroutableArtifact = stopOnUnroutableArtifact;
    }

    public boolean failFast() { return failFast; }
    public boolean stopOnUnroutableArtifact() { return stopOnUnroutableArtifact; }
}
