package org.texttechnologylab.duui.exception;

public abstract class PipelineException extends Exception {
    private final FailureCategory category;
    private final FailureScope scope;
    private final Recoverability recoverability;
    private final FailureSeverity severity;
    private final FailurePolicy policy;
    private final PipelineFailureContext failureContext;

    protected PipelineException(
            String message,
            Throwable cause,
            FailureCategory category,
            FailureScope scope,
            Recoverability recoverability,
            FailureSeverity severity,
            FailurePolicy policy,
            PipelineFailureContext failureContext
    ) {
        super(message, cause);
        this.category = category;
        this.scope = scope == null ? FailureScope.STAGE : scope;
        this.recoverability = recoverability == null ? Recoverability.NON_RETRYABLE : recoverability;
        this.severity = severity == null ? FailureSeverity.ERROR : severity;
        this.policy = policy == null ? defaultPolicy(this.recoverability, this.severity) : policy;
        this.failureContext = failureContext == null ? PipelineFailureContext.builder().build() : failureContext;
    }

    protected PipelineException(
            String message,
            FailureCategory category,
            FailureScope scope,
            Recoverability recoverability,
            FailureSeverity severity,
            FailurePolicy policy,
            PipelineFailureContext failureContext
    ) {
        this(message, null, category, scope, recoverability, severity, policy, failureContext);
    }

    private static FailurePolicy defaultPolicy(Recoverability recoverability, FailureSeverity severity) {
        if (severity == FailureSeverity.FATAL) {
            return FailurePolicy.FAIL_FAST;
        }
        if (recoverability == Recoverability.RETRYABLE_WITH_BACKOFF) {
            return FailurePolicy.exponentialBackoff(3, 500, 30_000);
        }
        if (recoverability == Recoverability.RETRYABLE_AFTER_THROTTLE) {
            return FailurePolicy.throttleAndRetry(3, 1_000, 30_000);
        }
        if (recoverability == Recoverability.RETRYABLE) {
            return FailurePolicy.retry(3);
        }
        return FailurePolicy.FAIL_FAST;
    }

    public FailureCategory getCategory() { return category; }
    public FailureScope getScope() { return scope; }
    public Recoverability getRecoverability() { return recoverability; }
    public FailureSeverity getSeverity() { return severity; }
    public FailurePolicy getPolicy() { return policy; }
    public PipelineFailureContext getFailureContext() { return failureContext; }

    public boolean isRetryable() {
        return recoverability == Recoverability.RETRYABLE
                || recoverability == Recoverability.RETRYABLE_WITH_BACKOFF
                || recoverability == Recoverability.RETRYABLE_AFTER_THROTTLE;
    }

    public boolean isFatal() {
        return severity == FailureSeverity.FATAL
                || policy.getAction() == FailureAction.FAIL_FAST
                || policy.getAction() == FailureAction.CANCEL_IMPORT;
    }
}
