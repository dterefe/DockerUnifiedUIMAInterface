package org.texttechnologylab.duui.artifact;

import java.time.Instant;

public final class DUUIArtifactState {
    private int attempt;
    private boolean cancelled;
    private boolean degraded;
    private boolean complete;
    private String checkpointId;
    private Instant updatedAt = Instant.now();

    public int attempt() { return attempt; }
    public boolean cancelled() { return cancelled; }
    public boolean degraded() { return degraded; }
    public boolean complete() { return complete; }
    public String checkpointId() { return checkpointId; }
    public Instant updatedAt() { return updatedAt; }

    public void incrementAttempt() { attempt++; touch(); }
    public void markCancelled() { cancelled = true; touch(); }
    public void markDegraded() { degraded = true; touch(); }
    public void markComplete() { complete = true; touch(); }
    public void checkpointId(String checkpointId) { this.checkpointId = checkpointId; touch(); }

    private void touch() { updatedAt = Instant.now(); }
}
