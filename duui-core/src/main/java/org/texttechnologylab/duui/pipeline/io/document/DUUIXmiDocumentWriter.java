package org.texttechnologylab.duui.pipeline.io.document;

import org.apache.uima.cas.SerialFormat;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.filesystem.DUUIDocumentClient;
import org.texttechnologylab.duui.pipeline.DUUITarget;
import org.texttechnologylab.duui.runtime.DUUIFlowScope;
import org.texttechnologylab.duui.runtime.DUUITargetScope;

import java.util.Objects;
import java.util.function.Function;

public final class DUUIXmiDocumentWriter implements DUUITarget<JCas> {
    private final DUUIDocumentWriter<JCas> delegate;

    private DUUIXmiDocumentWriter(Builder builder) {
        DUUIAddress outputDirectory = Objects.requireNonNull(builder.outputDirectory, "outputDirectory");
        DUUIDocumentWriter.Builder<JCas> writer = DUUIDocumentWriter
                .builder(new DUUIXmiDocumentSerializer(builder.format))
                .client(builder.client)
                .address(artifact -> child(outputDirectory, builder.fileName.apply(artifact)));
        this.delegate = writer.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void accept(DUUIArtifact<JCas> artifact) throws Exception {
        delegate.accept(artifact);
    }

    public DUUITargetScope<JCas> open(DUUIFlowScope<JCas> parent) {
        return parent.pipeline().target(parent, this);
    }

    private static DUUIAddress child(DUUIAddress directory, String fileName) {
        String directoryPath = directory.path();
        String path = directoryPath == null || directoryPath.isBlank()
                ? fileName
                : directoryPath.endsWith("/") ? directoryPath + fileName : directoryPath + "/" + fileName;
        return new DUUIAddress(directory.scheme(), directory.authority(), path,
                directory.query(), directory.fragment());
    }

    public static final class Builder {
        private DUUIDocumentClient client;
        private DUUIAddress outputDirectory;
        private SerialFormat format = SerialFormat.XMI_1_1_PRETTY;
        private Function<DUUIArtifact<JCas>, String> fileName = artifact -> artifact.gid().value() + ".xmi";

        private Builder() {
        }

        public Builder client(DUUIDocumentClient client) {
            this.client = Objects.requireNonNull(client, "client");
            return this;
        }

        public Builder output(DUUIAddress outputDirectory) {
            this.outputDirectory = Objects.requireNonNull(outputDirectory, "outputDirectory");
            return this;
        }

        public Builder format(SerialFormat format) {
            this.format = Objects.requireNonNull(format, "format");
            return this;
        }

        public Builder fileName(Function<DUUIArtifact<JCas>, String> fileName) {
            this.fileName = Objects.requireNonNull(fileName, "fileName");
            return this;
        }

        public DUUIXmiDocumentWriter build() {
            return new DUUIXmiDocumentWriter(this);
        }

        public DUUITargetScope<JCas> open(DUUIFlowScope<JCas> parent) {
            return build().open(parent);
        }
    }
}
