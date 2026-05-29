package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.pipeline.component.DUUIAnnotator;

import java.util.Objects;

public final class DUUILambda<T> implements DUUIAnnotator<T> {
    private final GID gid;
    private final DUUITraits traits;
    private final String id;
    private final DUUIProcessor<T> processor;

    private DUUILambda(String id, DUUIProcessor<T> processor) {
        this.gid = GID.create();
        this.traits = DUUITraits.empty();
        this.id = Objects.requireNonNull(id, "id");
        this.processor = Objects.requireNonNull(processor, "processor");
    }

    public static <T> Builder<T> builder(String id) {
        return new Builder<>(id);
    }

    @Override
    public GID gid() {
        return gid;
    }

    @Override
    public DUUITraits traits() {
        return traits;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public DUUIArtifact<T> process(DUUIArtifact<T> artifact) throws Exception {
        return processor.process(artifact);
    }

    public static final class Builder<T> {
        private final String id;
        private DUUIProcessor<T> processor;

        private Builder(String id) {
            this.id = Objects.requireNonNull(id, "id");
        }

        public Builder<T> processor(DUUIProcessor<T> processor) {
            this.processor = processor;
            return this;
        }

        public DUUILambda<T> build() {
            return new DUUILambda<>(id, processor);
        }
    }
}
