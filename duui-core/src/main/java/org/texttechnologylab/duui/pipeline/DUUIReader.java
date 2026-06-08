package org.texttechnologylab.duui.pipeline;

/**
 * Marker interface for processor stages that read/consume artifacts.
 * Semantically equivalent to an adapter from external input to artifact.
 *
 * [DESIGN: lines 1-2, 78-83]
 */
@FunctionalInterface
public interface DUUIReader<T> extends DUUIProcessor<T> {
}
