package org.texttechnologylab.duui.pipeline.component;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;

import java.util.Objects;

public record DUUINode<T>(String id, DUUINodeProcessor<T> processor, DUUIV1Annotator annotator) {
    public DUUINode {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(processor, "processor");
    }

    public DUUINode(String id, DUUINodeProcessor<T> processor) {
        this(id, processor, null);
    }

    public static DUUINode<JCas> v1(String id, DUUIV1Annotator annotator) {
        Objects.requireNonNull(annotator, "annotator");
        return new DUUINode<>(id, artifact -> {
            annotator.process(artifact);
            return artifact;
        }, annotator);
    }

    public DUUIArtifact<T> process(DUUIArtifact<T> artifact) throws Exception {
        return processor.process(artifact);
    }
}
