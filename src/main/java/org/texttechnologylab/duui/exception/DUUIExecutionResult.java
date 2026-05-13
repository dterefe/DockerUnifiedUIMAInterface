package org.texttechnologylab.duui.exception;

import org.texttechnologylab.duui.artifact.DUUIArtifact;

public final class DUUIExecutionResult<T> {
    private final DUUIArtifact<T> artifact;
    private final DUUIExecutionStatus status;
    private final DUUIFailure failure;
    private final long durationMs;
    private final int attempt;

    private DUUIExecutionResult(DUUIArtifact<T> artifact, DUUIExecutionStatus status, DUUIFailure failure, long durationMs, int attempt) {
        this.artifact = artifact;
        this.status = status;
        this.failure = failure;
        this.durationMs = durationMs;
        this.attempt = attempt;
    }

    public static <T> DUUIExecutionResult<T> success(DUUIArtifact<T> artifact, long durationMs, int attempt) {
        return new DUUIExecutionResult<>(artifact, DUUIExecutionStatus.SUCCESS, null, durationMs, attempt);
    }

    public static <T> DUUIExecutionResult<T> failure(DUUIArtifact<T> artifact, DUUIFailure failure, long durationMs, int attempt) {
        return new DUUIExecutionResult<>(artifact, DUUIExecutionStatus.FAILED, failure, durationMs, attempt);
    }

    public DUUIArtifact<T> artifact() { return artifact; }
    public DUUIExecutionStatus status() { return status; }
    public DUUIFailure failure() { return failure; }
    public long durationMs() { return durationMs; }
    public int attempt() { return attempt; }
}
