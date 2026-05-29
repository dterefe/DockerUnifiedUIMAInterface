package org.texttechnologylab.duui.exception;

public class ConcurrencyFailureException extends PipelineException {
    public ConcurrencyFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.CONCURRENCY, scope, Recoverability.RETRYABLE_WITH_BACKOFF, FailureSeverity.ERROR, FailurePolicy.exponentialBackoff(3, 250, 5_000), context);
    }

    public ConcurrencyFailureException(String message, Throwable cause, FailureScope scope, FailurePolicy policy, PipelineFailureContext context) {
        super(message, cause, FailureCategory.CONCURRENCY, scope, Recoverability.RETRYABLE_WITH_BACKOFF, FailureSeverity.ERROR, policy, context);
    }
}
