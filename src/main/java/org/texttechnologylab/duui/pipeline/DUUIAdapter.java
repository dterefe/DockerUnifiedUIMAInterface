package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;

public interface DUUIAdapter<A, B> {
    DUUIArtifactType<A> inputType();

    DUUIArtifactType<B> outputType();

    DUUIArtifact<B> adapt(DUUIArtifact<A> artifact) throws Exception;

    default DUUIArtifact<B> successor(DUUIArtifact<A> artifact, B payload) {
        return artifact.successorArtifact(payload, outputType());
    }
}
