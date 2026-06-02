package org.texttechnologylab.duui.pipeline.component;

import org.texttechnologylab.duui.pipeline.DUUIProcessor;

import java.util.Objects;

public record DUUINode<T>(String id, DUUIProcessor<T> processor, DUUIAnnotator<T> annotator) {
    public DUUINode {
        Objects.requireNonNull(id, "id");
        if (processor == null && annotator == null) {
            throw new NullPointerException("processor or annotator");
        }
    }

    public DUUINode(String id, DUUIProcessor<T> processor) {
        this(id, processor, null);
    }

}
