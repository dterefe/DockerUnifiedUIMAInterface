package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.exception.DUUIExecutionStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class DUUIOrchestrationResult {
    private final List<DUUIExecutionResult<?>> results = new ArrayList<>();
    private final List<DUUIArtifact<?>> unroutableArtifacts = new ArrayList<>();

    public void addResult(DUUIExecutionResult<?> result) {
        if (result != null) results.add(result);
    }

    public void addUnroutableArtifact(DUUIArtifact<?> artifact) {
        if (artifact != null) unroutableArtifacts.add(artifact);
    }

    public boolean hasFailures() {
        return results.stream().anyMatch(result -> result.status() == DUUIExecutionStatus.FAILED);
    }

    public List<DUUIExecutionResult<?>> results() { return Collections.unmodifiableList(results); }
    public List<DUUIArtifact<?>> unroutableArtifacts() { return Collections.unmodifiableList(unroutableArtifacts); }
}
