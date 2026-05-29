package org.texttechnologylab.duui.exception;

public class ConsistencyFailureException extends PipelineException {
    public ConsistencyFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.CONSISTENCY, scope, Recoverability.COMPENSATABLE, FailureSeverity.ERROR, new FailurePolicy(FailureAction.MARK_INCOMPLETE, 1, BackoffStrategy.NONE, 0, 0, false), context);
    }

    public ConsistencyFailureException(String message, Throwable cause, FailureScope scope, FailureSeverity severity, FailurePolicy policy, PipelineFailureContext context) {
        super(message, cause, FailureCategory.CONSISTENCY, scope, Recoverability.COMPENSATABLE, severity, policy, context);
    }
}
