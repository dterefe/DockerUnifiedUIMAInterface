package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;

public interface DUUISplit<I, O> {
    void split(DUUIArtifact<I> artifact, DUUIArtifactEmitter<O> emitter) throws Exception;

    default DUUIArtifact<O> part(DUUIArtifact<I> artifact, O payload) {
        return DUUIArtifact.of(payload);
    }
}
