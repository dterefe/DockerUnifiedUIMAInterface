package org.texttechnologylab.duui.exception;

public class TransientInfrastructureFailureException extends PipelineException {
    public TransientInfrastructureFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.TRANSIENT_INFRASTRUCTURE, scope, Recoverability.RETRYABLE_WITH_BACKOFF, FailureSeverity.ERROR, FailurePolicy.exponentialBackoff(5, 500, 30_000), context);
    }

    public TransientInfrastructureFailureException(String message, Throwable cause, FailureScope scope, FailurePolicy policy, PipelineFailureContext context) {
        super(message, cause, FailureCategory.TRANSIENT_INFRASTRUCTURE, scope, Recoverability.RETRYABLE_WITH_BACKOFF, FailureSeverity.ERROR, policy, context);
    }
}
