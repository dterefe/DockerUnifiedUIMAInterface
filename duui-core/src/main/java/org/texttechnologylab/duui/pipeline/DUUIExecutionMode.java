package org.texttechnologylab.duui.pipeline;

/**
 * Legacy execution mode for backward compatibility.
 * Use {@link DUUIStageType#LINEAR_PROCESSOR} and {@link DUUIStageType#PARALLEL_PROCESSOR} instead.
 *
 * [DESIGN: lines 80-82]
 *
 * @deprecated Use DUUIStageType.LINEAR_PROCESSOR / PARALLEL_PROCESSOR directly.
 */
@Deprecated
public enum DUUIExecutionMode {
    LINEAR,
    PARALLEL
}
