package org.texttechnologylab.duui.pipeline.component;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.pipeline.DUUIProcessor;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;

import java.util.Objects;

public record DUUINode<T>(String id, DUUIProcessor<T> processor, DUUIAnnotator<T> annotator) {
    public DUUINode {
        Objects.requireNonNull(id, "id");
        if (processor == null && !(annotator instanceof DUUIV1Annotator)) {
            throw new NullPointerException("processor");
        }
    }

    public DUUINode(String id, DUUIProcessor<T> processor) {
        this(id, processor, null);
    }

    public static DUUINode<JCas> v1(String id, DUUIV1Annotator annotator) {
        Objects.requireNonNull(annotator, "annotator");
        return new DUUINode<>(id, null, annotator);
    }

}
