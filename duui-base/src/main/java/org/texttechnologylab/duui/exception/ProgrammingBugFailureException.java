package org.texttechnologylab.duui.exception;

public class ProgrammingBugFailureException extends PipelineException {
    public ProgrammingBugFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.PROGRAMMING_BUG, scope, Recoverability.NON_RETRYABLE, FailureSeverity.FATAL, FailurePolicy.FAIL_FAST, context);
    }
}
