package org.texttechnologylab.duui.exception;

/**
 * Error handling policy for DUUI components and stages.
 *
 * <p>[DESIGN: lines 431-460]</p>
 * <ul>
 *   <li>{@link #RETRY} — retry the operation up to the configured maximum attempts</li>
 *   <li>{@link #SKIP} — skip the failed artifact and continue to the next checkpoint</li>
 *   <li>{@link #FAIL} — fail immediately (fail-fast)</li>
 *   <li>{@link #DELEGATE} — delegate to the next higher-level policy (pipeline → stage → component)</li>
 * </ul>
 */
public enum DUUIErrorPolicy {
    RETRY,
    SKIP,
    FAIL,
    DELEGATE
}
