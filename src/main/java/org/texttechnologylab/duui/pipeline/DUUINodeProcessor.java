package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;

@FunctionalInterface
public interface DUUINodeProcessor<T> {
    DUUIArtifact<T> process(DUUIArtifact<T> artifact) throws Exception;
}
