package org.texttechnologylab.duui.exception;

public class CancellationFailureException extends PipelineException {
    public CancellationFailureException(String message, FailureScope scope, PipelineFailureContext context) {
        super(message, FailureCategory.CANCELLATION, scope, Recoverability.RESUMABLE, FailureSeverity.WARNING, new FailurePolicy(FailureAction.CHECKPOINT_AND_STOP, 1, BackoffStrategy.NONE, 0, 0, false), context);
    }

    public CancellationFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.CANCELLATION, scope, Recoverability.RESUMABLE, FailureSeverity.WARNING, new FailurePolicy(FailureAction.CHECKPOINT_AND_STOP, 1, BackoffStrategy.NONE, 0, 0, false), context);
    }
}
