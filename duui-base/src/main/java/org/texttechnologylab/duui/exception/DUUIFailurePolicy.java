package org.texttechnologylab.duui.exception;

public final class DUUIFailurePolicy {
    public static final DUUIFailurePolicy FAIL_FAST = new DUUIFailurePolicy(DUUIFailureAction.FAIL_FAST, 1, DUUIBackoffStrategy.NONE, 0, 0, false);
    public static final DUUIFailurePolicy CONTINUE = new DUUIFailurePolicy(DUUIFailureAction.CONTINUE, 1, DUUIBackoffStrategy.NONE, 0, 0, false);
    public static final DUUIFailurePolicy SKIP_ARTIFACT = new DUUIFailurePolicy(DUUIFailureAction.SKIP_ARTIFACT, 1, DUUIBackoffStrategy.NONE, 0, 0, false);

    private final DUUIFailureAction action;
    private final int maxAttempts;
    private final DUUIBackoffStrategy backoffStrategy;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final boolean jitter;

    public DUUIFailurePolicy(DUUIFailureAction action, int maxAttempts, DUUIBackoffStrategy backoffStrategy, long initialBackoffMs, long maxBackoffMs, boolean jitter) {
        this.action = action == null ? DUUIFailureAction.FAIL_FAST : action;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.backoffStrategy = backoffStrategy == null ? DUUIBackoffStrategy.NONE : backoffStrategy;
        this.initialBackoffMs = Math.max(0, initialBackoffMs);
        this.maxBackoffMs = Math.max(this.initialBackoffMs, maxBackoffMs);
        this.jitter = jitter;
    }

    public static DUUIFailurePolicy retry(int maxAttempts) {
        return new DUUIFailurePolicy(DUUIFailureAction.RETRY, maxAttempts, DUUIBackoffStrategy.NONE, 0, 0, false);
    }

    public static DUUIFailurePolicy backoffAndRetry(int maxAttempts, long initialBackoffMs, long maxBackoffMs) {
        return new DUUIFailurePolicy(DUUIFailureAction.BACKOFF_AND_RETRY, maxAttempts, DUUIBackoffStrategy.EXPONENTIAL_WITH_JITTER, initialBackoffMs, maxBackoffMs, true);
    }

    public static DUUIFailurePolicy throttleAndRetry(int maxAttempts, long initialBackoffMs, long maxBackoffMs) {
        return new DUUIFailurePolicy(DUUIFailureAction.THROTTLE_AND_RETRY, maxAttempts, DUUIBackoffStrategy.EXPONENTIAL_WITH_JITTER, initialBackoffMs, maxBackoffMs, true);
    }

    public DUUIFailureAction action() { return action; }
    public int maxAttempts() { return maxAttempts; }
    public DUUIBackoffStrategy backoffStrategy() { return backoffStrategy; }
    public long initialBackoffMs() { return initialBackoffMs; }
    public long maxBackoffMs() { return maxBackoffMs; }
    public boolean jitter() { return jitter; }
}
