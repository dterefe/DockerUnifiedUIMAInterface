package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;

public interface DUUILambda<T> {
    DUUIArtifactType<T> inputType();

    DUUIArtifact<T> process(DUUIArtifact<T> artifact) throws Exception;
}
