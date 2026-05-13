package org.texttechnologylab.duui.exception;

public class ValidationFailureException extends PipelineException {
    public ValidationFailureException(String message, FailureScope scope, PipelineFailureContext context) {
        super(message, FailureCategory.VALIDATION, scope, Recoverability.NON_RETRYABLE, FailureSeverity.ERROR, FailurePolicy.FAIL_FAST, context);
    }

    public ValidationFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.VALIDATION, scope, Recoverability.NON_RETRYABLE, FailureSeverity.ERROR, FailurePolicy.FAIL_FAST, context);
    }
}
