package org.texttechnologylab.duui.exception;

public class ResourceExhaustionFailureException extends PipelineException {
    public ResourceExhaustionFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.RESOURCE_EXHAUSTION, scope, Recoverability.RETRYABLE_AFTER_THROTTLE, FailureSeverity.ERROR, FailurePolicy.throttleAndRetry(3, 1_000, 30_000), context);
    }

    public ResourceExhaustionFailureException(String message, Throwable cause, FailureScope scope, FailureSeverity severity, FailurePolicy policy, PipelineFailureContext context) {
        super(message, cause, FailureCategory.RESOURCE_EXHAUSTION, scope, Recoverability.RETRYABLE_AFTER_THROTTLE, severity, policy, context);
    }
}
