package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;

import java.util.List;

public interface DUUIJoin<I, O> {
    DUUIArtifact<O> join(List<DUUIArtifact<I>> artifacts) throws Exception;
}
