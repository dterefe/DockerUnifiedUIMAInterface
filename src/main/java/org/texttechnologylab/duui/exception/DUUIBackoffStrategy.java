package org.texttechnologylab.duui.exception;

public enum DUUIBackoffStrategy {
    NONE,
    FIXED,
    LINEAR,
    EXPONENTIAL,
    EXPONENTIAL_WITH_JITTER,
    DECORRELATED_JITTER,
    DEADLINE_BASED
}
