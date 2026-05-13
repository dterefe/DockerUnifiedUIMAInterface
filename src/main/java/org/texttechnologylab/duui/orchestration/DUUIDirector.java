package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;

import java.util.Optional;

public interface DUUIDirector {
    Optional<DUUICheckpoint<?>> checkpointFor(DUUIPipeline pipeline, DUUIArtifact<?> artifact);
}
