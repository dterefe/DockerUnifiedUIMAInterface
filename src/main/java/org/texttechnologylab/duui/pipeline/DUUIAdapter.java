package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;

public interface DUUIAdapter<A, B> {
    DUUIArtifact<B> adapt(DUUIArtifact<A> artifact) throws Exception;

    default DUUIArtifact<B> successor(DUUIArtifact<A> artifact, B payload) {
        return DUUIArtifact.of(payload);
    }
}
