package org.texttechnologylab.duui.exception;

public class ConflictFailureException extends PipelineException {
    public ConflictFailureException(String message, FailureScope scope, FailurePolicy policy, PipelineFailureContext context) {
        super(message, FailureCategory.CONFLICT, scope, Recoverability.RESUMABLE, FailureSeverity.WARNING, policy, context);
    }

    public ConflictFailureException(String message, Throwable cause, FailureScope scope, FailurePolicy policy, PipelineFailureContext context) {
        super(message, cause, FailureCategory.CONFLICT, scope, Recoverability.RESUMABLE, FailureSeverity.WARNING, policy, context);
    }
}
