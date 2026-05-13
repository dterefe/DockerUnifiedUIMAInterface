package org.texttechnologylab.duui.pipeline.io;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasIOUtils;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactContext;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public final class DUUIXmiCollectionReader implements DUUIGenerator<JCas> {
    private final DUUIArtifactType<JCas> outputType;
    private final List<Path> sources;
    private final TypeSystemDescription typeSystemDescription;

    private DUUIXmiCollectionReader(Builder builder) {
        this.outputType = Objects.requireNonNull(builder.outputType, "outputType");
        this.sources = List.copyOf(builder.sources);
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("DUUI XMI collection reader requires at least one XMI source.");
        }
        this.typeSystemDescription = builder.typeSystemDescription;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public DUUIArtifactType<JCas> outputType() {
        return outputType;
    }

    @Override
    public void generate(DUUIArtifactEmitter<JCas> emitter) throws Exception {
        for (Path source : sources) {
            JCas cas = createCas();
            try (InputStream input = Files.newInputStream(source)) {
                CasIOUtils.load(input, cas.getCas());
            }
            emitter.emit(DUUIArtifact.of(cas, outputType).withContext(
                    DUUIArtifactContext.builder()
                            .sourcePath(source.toAbsolutePath().toString())
                            .sourceArtifactId(source.toAbsolutePath().toString())
                            .build()
            ));
        }
    }

    public DUUIGeneratorScope<JCas> open(DUUIPipelineScope pipeline) {
        return pipeline.add(this);
    }

    private JCas createCas() throws ResourceInitializationException, CASException {
        if (typeSystemDescription == null) {
            return JCasFactory.createJCas();
        }
        return JCasFactory.createJCas(typeSystemDescription);
    }

    public static final class Builder {
        private DUUIArtifactType<JCas> outputType = DUUIArtifactType.of("duui/xmi/jcas");
        private final List<Path> sources = new ArrayList<>();
        private TypeSystemDescription typeSystemDescription;

        private Builder() {
        }

        public Builder artifactType(DUUIArtifactType<JCas> outputType) {
            this.outputType = Objects.requireNonNull(outputType, "outputType");
            return this;
        }

        public Builder typeSystem(TypeSystemDescription typeSystemDescription) {
            this.typeSystemDescription = typeSystemDescription;
            return this;
        }

        public Builder source(Path source) throws java.io.IOException {
            Objects.requireNonNull(source, "source");
            if (Files.isDirectory(source)) {
                try (Stream<Path> stream = Files.list(source)) {
                    stream
                            .filter(Files::isRegularFile)
                            .filter(path -> path.getFileName().toString().endsWith(".xmi"))
                            .sorted(Comparator.comparing(Path::toString))
                            .forEach(sources::add);
                }
            } else {
                sources.add(source);
            }
            return this;
        }

        public DUUIXmiCollectionReader build() {
            return new DUUIXmiCollectionReader(this);
        }

        public DUUIGeneratorScope<JCas> open(DUUIPipelineScope pipeline) {
            return build().open(pipeline);
        }
    }
}
