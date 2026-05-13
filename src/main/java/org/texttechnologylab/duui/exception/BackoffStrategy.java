package org.texttechnologylab.duui.exception;

public enum BackoffStrategy {
    NONE,
    FIXED,
    LINEAR,
    EXPONENTIAL,
    EXPONENTIAL_WITH_JITTER,
    DECORRELATED_JITTER,
    DEADLINE_BASED
}
