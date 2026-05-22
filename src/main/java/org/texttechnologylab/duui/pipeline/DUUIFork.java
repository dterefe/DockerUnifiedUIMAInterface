package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;

public interface DUUIFork<P, C> {
    void fork(DUUIArtifact<P> artifact, DUUIArtifactEmitter<C> emitter) throws Exception;

    default DUUIArtifact<C> child(DUUIArtifact<P> artifact, C payload) {
        return DUUIArtifact.of(payload);
    }
}
