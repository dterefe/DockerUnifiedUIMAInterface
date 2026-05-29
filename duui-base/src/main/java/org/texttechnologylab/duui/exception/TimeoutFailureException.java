package org.texttechnologylab.duui.exception;

public class TimeoutFailureException extends PipelineException {
    public TimeoutFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.TIMEOUT, scope, Recoverability.RETRYABLE_WITH_BACKOFF, FailureSeverity.ERROR, FailurePolicy.exponentialBackoff(3, 500, 15_000), context);
    }

    public TimeoutFailureException(String message, Throwable cause, FailureScope scope, FailurePolicy policy, PipelineFailureContext context) {
        super(message, cause, FailureCategory.TIMEOUT, scope, Recoverability.RETRYABLE_WITH_BACKOFF, FailureSeverity.ERROR, policy, context);
    }
}
