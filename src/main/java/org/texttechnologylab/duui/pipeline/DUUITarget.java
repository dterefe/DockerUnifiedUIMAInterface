package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;

public interface DUUITarget<T> {
    DUUIArtifactType<T> inputType();

    void accept(DUUIArtifact<T> artifact) throws Exception;
}
