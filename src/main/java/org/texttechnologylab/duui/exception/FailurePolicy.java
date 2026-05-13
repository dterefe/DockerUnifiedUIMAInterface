package org.texttechnologylab.duui.exception;

public final class FailurePolicy {
    public static final FailurePolicy FAIL_FAST = new FailurePolicy(FailureAction.FAIL_FAST, 1, BackoffStrategy.NONE, 0, 0, false);
    public static final FailurePolicy WARN_AND_CONTINUE = new FailurePolicy(FailureAction.CONTINUE, 1, BackoffStrategy.NONE, 0, 0, false);
    public static final FailurePolicy SKIP_DOCUMENT = new FailurePolicy(FailureAction.SKIP_DOCUMENT, 1, BackoffStrategy.NONE, 0, 0, false);

    private final FailureAction action;
    private final int maxAttempts;
    private final BackoffStrategy backoffStrategy;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final boolean jitter;

    public FailurePolicy(
            FailureAction action,
            int maxAttempts,
            BackoffStrategy backoffStrategy,
            long initialBackoffMs,
            long maxBackoffMs,
            boolean jitter
    ) {
        this.action = action == null ? FailureAction.FAIL_FAST : action;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.backoffStrategy = backoffStrategy == null ? BackoffStrategy.NONE : backoffStrategy;
        this.initialBackoffMs = Math.max(0, initialBackoffMs);
        this.maxBackoffMs = Math.max(this.initialBackoffMs, maxBackoffMs);
        this.jitter = jitter;
    }

    public static FailurePolicy retry(int maxAttempts) {
        return new FailurePolicy(FailureAction.RETRY_STAGE, maxAttempts, BackoffStrategy.NONE, 0, 0, false);
    }

    public static FailurePolicy exponentialBackoff(int maxAttempts, long initialBackoffMs, long maxBackoffMs) {
        return new FailurePolicy(
                FailureAction.BACKOFF_AND_RETRY,
                maxAttempts,
                BackoffStrategy.EXPONENTIAL_WITH_JITTER,
                initialBackoffMs,
                maxBackoffMs,
                true
        );
    }

    public static FailurePolicy throttleAndRetry(int maxAttempts, long initialBackoffMs, long maxBackoffMs) {
        return new FailurePolicy(
                FailureAction.THROTTLE_AND_RETRY,
                maxAttempts,
                BackoffStrategy.EXPONENTIAL_WITH_JITTER,
                initialBackoffMs,
                maxBackoffMs,
                true
        );
    }

    public FailureAction getAction() { return action; }
    public int getMaxAttempts() { return maxAttempts; }
    public BackoffStrategy getBackoffStrategy() { return backoffStrategy; }
    public long getInitialBackoffMs() { return initialBackoffMs; }
    public long getMaxBackoffMs() { return maxBackoffMs; }
    public boolean isJitter() { return jitter; }
}
