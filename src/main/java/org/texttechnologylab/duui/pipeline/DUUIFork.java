package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;

public interface DUUIFork<P, C> {
    DUUIArtifactType<P> inputType();

    DUUIArtifactType<C> outputType();

    void fork(DUUIArtifact<P> artifact, DUUIArtifactEmitter<C> emitter) throws Exception;

    default DUUIArtifact<C> child(DUUIArtifact<P> artifact, C payload) {
        return artifact.childArtifact(payload, outputType());
    }
}
