package org.texttechnologylab.duui.pipeline.io.document;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.filesystem.DUUIDocumentClient;
import org.texttechnologylab.duui.pipeline.DUUITarget;
import org.texttechnologylab.duui.runtime.DUUIFlowScope;
import org.texttechnologylab.duui.runtime.DUUITargetScope;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Objects;
import java.util.function.Function;

public final class DUUIDocumentWriter<T> implements DUUITarget<T> {
    private final DUUIDocumentClient client;
    private final DUUIDocumentSerializer<T> serializer;
    private final Function<DUUIArtifact<T>, DUUIAddress> address;

    private DUUIDocumentWriter(Builder<T> builder) {
        this.client = Objects.requireNonNull(builder.client, "client");
        this.serializer = Objects.requireNonNull(builder.serializer, "serializer");
        this.address = Objects.requireNonNull(builder.address, "address");
    }

    public static <T> Builder<T> builder(DUUIDocumentSerializer<T> serializer) {
        return new Builder<>(serializer);
    }

    @Override
    public void accept(DUUIArtifact<T> artifact) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.write(artifact, output);
        client.write(address.apply(artifact), new ByteArrayInputStream(output.toByteArray()));
    }

    public DUUITargetScope<T> open(DUUIFlowScope<T> parent) {
        return parent.pipeline().target(parent, this);
    }

    public static final class Builder<T> {
        private final DUUIDocumentSerializer<T> serializer;
        private DUUIDocumentClient client;
        private Function<DUUIArtifact<T>, DUUIAddress> address;

        private Builder(DUUIDocumentSerializer<T> serializer) {
            this.serializer = Objects.requireNonNull(serializer, "serializer");
        }

        public Builder<T> client(DUUIDocumentClient client) {
            this.client = Objects.requireNonNull(client, "client");
            return this;
        }

        public Builder<T> address(Function<DUUIArtifact<T>, DUUIAddress> address) {
            this.address = Objects.requireNonNull(address, "address");
            return this;
        }

        public DUUIDocumentWriter<T> build() {
            return new DUUIDocumentWriter<>(this);
        }
    }
}
