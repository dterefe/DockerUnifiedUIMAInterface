package org.texttechnologylab.duui.pipeline.component;

import org.texttechnologylab.duui.pipeline.DUUIProcessor;

import java.util.Objects;

/**
 * Generic concurrency slot primitive.
 * Each annotator can have {@code replica} copies, each with {@code concurrency} slots.
 * Capacity = replica × concurrency.
 *
 * [DESIGN: line 288]
 */
public record DUUINode<T>(String id, DUUIProcessor<T> processor, DUUIAnnotator<T> annotator, int replica, int concurrency) {
    public DUUINode {
        Objects.requireNonNull(id, "id");
        if (processor == null && annotator == null) {
            throw new NullPointerException("processor or annotator");
        }
        if (replica < 1) {
            throw new IllegalArgumentException("replica must be >= 1, got: " + replica);
        }
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >= 1, got: " + concurrency);
        }
    }

    public DUUINode(String id, DUUIProcessor<T> processor) {
        this(id, processor, null, 1, 1);
    }

    public DUUINode(String id, DUUIProcessor<T> processor, DUUIAnnotator<T> annotator) {
        this(id, processor, annotator, 1, 1);
    }

    /**
     * Total capacity = replica × concurrency.
     * [DESIGN: line 288]
     */
    public int capacity() {
        return replica * concurrency;
    }
}
