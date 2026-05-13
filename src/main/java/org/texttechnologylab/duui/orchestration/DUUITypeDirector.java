package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;

import java.util.Optional;

public final class DUUITypeDirector implements DUUIDirector {
    @Override
    public Optional<DUUICheckpoint<?>> checkpointFor(DUUIPipeline pipeline, DUUIArtifact<?> artifact) {
        if (pipeline == null || artifact == null) {
            return Optional.empty();
        }
        return pipeline.checkpoints().stream()
                .filter(checkpoint -> checkpoint.artifactType().accepts(artifact.artifactType()))
                .findFirst();
    }
}
