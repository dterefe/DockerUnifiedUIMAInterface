package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;

public interface DUUITarget<T> {
    void accept(DUUIArtifact<T> artifact) throws Exception;
}
