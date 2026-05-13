package org.texttechnologylab.duui.exception;

public enum Recoverability {
    NON_RETRYABLE,
    RETRYABLE,
    RETRYABLE_WITH_BACKOFF,
    RETRYABLE_AFTER_THROTTLE,
    RESUMABLE,
    COMPENSATABLE
}
