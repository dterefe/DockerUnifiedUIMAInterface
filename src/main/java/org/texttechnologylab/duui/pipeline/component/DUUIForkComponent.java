package org.texttechnologylab.duui.pipeline.component;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorker;
import org.texttechnologylab.duui.pipeline.DUUIFork;

final class DUUIForkComponent<P, C> extends DUUIComponent<P> {
    private final DUUIFork<P, C> fork;

    DUUIForkComponent(String id, DUUIFork<P, C> fork) {
        super(id, java.util.List.of(new DUUINode<>(id + "-slot-0", artifact -> artifact)));
        this.fork = fork;
    }

    @Override
    public DUUIArtifact<P> process(DUUIArtifact<P> artifact) throws Exception {
        fork.fork(artifact, emitted -> DUUIWorker.current().requireCurrentTask().context().emit(emitted));
        return artifact;
    }

    @Override
    public boolean fork() {
        return true;
    }
}
