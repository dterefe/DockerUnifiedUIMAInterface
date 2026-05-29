package org.texttechnologylab.duui.exception;

public class SemanticDataFailureException extends PipelineException {
    public SemanticDataFailureException(String message, FailureScope scope, PipelineFailureContext context) {
        super(message, FailureCategory.SEMANTIC_DATA, scope, Recoverability.NON_RETRYABLE, FailureSeverity.ERROR, FailurePolicy.SKIP_DOCUMENT, context);
    }

    public SemanticDataFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.SEMANTIC_DATA, scope, Recoverability.NON_RETRYABLE, FailureSeverity.ERROR, FailurePolicy.SKIP_DOCUMENT, context);
    }
}
