package org.texttechnologylab.duui.exception;

public class PersistentInfrastructureFailureException extends PipelineException {
    public PersistentInfrastructureFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.PERSISTENT_INFRASTRUCTURE, scope, Recoverability.NON_RETRYABLE, FailureSeverity.FATAL, FailurePolicy.FAIL_FAST, context);
    }
}
