package org.texttechnologylab.duui.pipeline;

/**
 * Stage type classification.
 *
 * [DESIGN: lines 76-92]
 */
public enum DUUIStageType {
    /**
     * Generates artifacts into the output pool. Must be the first stage in a pipeline.
     */
    SOURCE,
    /**
     * Processes artifacts through multiple components sequentially.
     * [DESIGN: line 81]
     */
    LINEAR_PROCESSOR,
    /**
     * Processes artifacts through multiple components in parallel.
     * [DESIGN: line 82]
     */
    PARALLEL_PROCESSOR,
    /**
     * Transforms artifact types. Non-mutating; creates new artifact wrapper.
     */
    ADAPTER,
    /**
     * Branches one artifact into N child artifacts of same type.
     */
    FORK,
    /**
     * Joins N child artifacts back into one parent artifact.
     */
    JOIN,
    /**
     * Terminal stage that writes artifacts to output. Nothing follows Target.
     */
    TARGET
}
