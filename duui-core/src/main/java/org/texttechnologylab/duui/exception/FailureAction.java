package org.texttechnologylab.duui.exception;

public enum FailureAction {
    CONTINUE,
    SKIP_DOCUMENT,
    SKIP_CORPUS,
    RETRY_STAGE,
    RETRY_BRANCH,
    BACKOFF_AND_RETRY,
    THROTTLE_AND_RETRY,
    CANCEL_DEPENDENTS,
    CANCEL_IMPORT,
    MARK_DEGRADED,
    MARK_INCOMPLETE,
    FAIL_FAST,
    COMPENSATE,
    CHECKPOINT_AND_STOP
}
