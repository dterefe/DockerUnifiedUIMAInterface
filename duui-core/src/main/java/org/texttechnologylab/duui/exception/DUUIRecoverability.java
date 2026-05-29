package org.texttechnologylab.duui.exception;

public enum DUUIRecoverability {
    NON_RETRYABLE,
    RETRYABLE,
    RETRYABLE_WITH_BACKOFF,
    RETRYABLE_AFTER_THROTTLE,
    RESUMABLE,
    COMPENSATABLE
}
