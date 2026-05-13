package org.texttechnologylab.duui.exception;

public enum DUUIFailureAction {
    CONTINUE,
    RETRY,
    BACKOFF_AND_RETRY,
    THROTTLE_AND_RETRY,
    SKIP_ARTIFACT,
    MARK_DEGRADED,
    MARK_INCOMPLETE,
    CANCEL_CHILDREN,
    CANCEL_IMPORT,
    CHECKPOINT_AND_STOP,
    FAIL_FAST
}
