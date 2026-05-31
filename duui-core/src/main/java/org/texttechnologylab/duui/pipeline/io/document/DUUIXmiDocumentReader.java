package org.texttechnologylab.duui.pipeline.io.document;

import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.filesystem.DUUIDocumentClient;
import org.texttechnologylab.duui.filesystem.DUUIFile;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public final class DUUIXmiDocumentReader implements DUUIGenerator<JCas> {
    private final DUUIDocumentReader<JCas> delegate;

    private DUUIXmiDocumentReader(Builder builder) {
        DUUIDocumentReader.Builder<JCas> reader = DUUIDocumentReader
                .builder(new DUUIXmiDocumentDeserializer(builder.typeSystemDescription))
                .client(builder.client)
                .filter(builder.filter);
        for (DUUIAddress source : builder.sources) {
            reader.source(source);
        }
        this.delegate = reader.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void generate(DUUIArtifactEmitter<JCas> emitter) throws Exception {
        delegate.generate(emitter);
    }

    public DUUIGeneratorScope<JCas> open(DUUIPipelineScope pipeline) {
        return pipeline.add(this);
    }

    private static boolean isXmi(DUUIFile file) {
        String name = file.name();
        return name.endsWith(".xmi") || name.endsWith(".xmi.bz2")
                || name.endsWith(".xmi.gz") || name.endsWith(".xmi.xz");
    }

    public static final class Builder {
        private final List<DUUIAddress> sources = new ArrayList<>();
        private DUUIDocumentClient client;
        private TypeSystemDescription typeSystemDescription;
        private Predicate<DUUIFile> filter = DUUIXmiDocumentReader::isXmi;

        private Builder() {
        }

        public Builder client(DUUIDocumentClient client) {
            this.client = Objects.requireNonNull(client, "client");
            return this;
        }

        public Builder typeSystem(TypeSystemDescription typeSystemDescription) {
            this.typeSystemDescription = typeSystemDescription;
            return this;
        }

        public Builder source(DUUIAddress source) {
            sources.add(Objects.requireNonNull(source, "source"));
            return this;
        }

        public Builder filter(Predicate<DUUIFile> filter) {
            this.filter = Objects.requireNonNull(filter, "filter");
            return this;
        }

        public DUUIXmiDocumentReader build() {
            return new DUUIXmiDocumentReader(this);
        }

        public DUUIGeneratorScope<JCas> open(DUUIPipelineScope pipeline) {
            return build().open(pipeline);
        }
    }
}
