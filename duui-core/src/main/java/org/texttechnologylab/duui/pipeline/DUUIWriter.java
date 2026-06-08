package org.texttechnologylab.duui.pipeline;

/**
 * Marker interface for processor stages that write/emit artifacts.
 * Semantically equivalent to an adapter from artifact to external output.
 *
 * [DESIGN: lines 1-2, 78-83]
 */
@FunctionalInterface
public interface DUUIWriter<T> extends DUUIProcessor<T> {
}
