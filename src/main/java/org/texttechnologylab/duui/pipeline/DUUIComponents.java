package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.orchestration.DUUIWorker;

public final class DUUIComponents {
    private DUUIComponents() {}

    public static <A, B> DUUIComponent<A> adapter(DUUIAdapter<A, B> adapter) {
        return DUUIComponent.processor("adapter", artifact -> {
            org.texttechnologylab.duui.artifact.DUUIArtifact<B> emitted = adapter.adapt(artifact);
            DUUIWorker.current().requireCurrentTask().context().emit(emitted);
            return artifact;
        });
    }

    public static <P, C> DUUIComponent<P> fork(DUUIFork<P, C> fork) {
        return new DUUIForkComponent<>("fork", fork);
    }

    public static <T> DUUIComponent<T> target(DUUITarget<T> target) {
        return DUUIComponent.processor("target", artifact -> {
            target.accept(artifact);
            return artifact;
        });
    }
}
