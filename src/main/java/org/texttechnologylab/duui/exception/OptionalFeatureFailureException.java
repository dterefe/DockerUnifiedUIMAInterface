package org.texttechnologylab.duui.exception;

public class OptionalFeatureFailureException extends PipelineException {
    public OptionalFeatureFailureException(String message, Throwable cause, FailureScope scope, PipelineFailureContext context) {
        super(message, cause, FailureCategory.OPTIONAL_FEATURE, scope, Recoverability.RESUMABLE, FailureSeverity.DEGRADED, new FailurePolicy(FailureAction.MARK_DEGRADED, 1, BackoffStrategy.NONE, 0, 0, false), context);
    }

    public OptionalFeatureFailureException(String message, Throwable cause, FailureScope scope, FailurePolicy policy, PipelineFailureContext context) {
        super(message, cause, FailureCategory.OPTIONAL_FEATURE, scope, Recoverability.RESUMABLE, FailureSeverity.DEGRADED, policy, context);
    }
}
