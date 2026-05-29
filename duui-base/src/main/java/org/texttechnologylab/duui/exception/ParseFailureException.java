package org.texttechnologylab.duui.exception;

public class ParseFailureException extends PipelineException {
    public ParseFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.PARSE, scope, Recoverability.NON_RETRYABLE, FailureSeverity.ERROR, FailurePolicy.SKIP_DOCUMENT, context);
    }
}
