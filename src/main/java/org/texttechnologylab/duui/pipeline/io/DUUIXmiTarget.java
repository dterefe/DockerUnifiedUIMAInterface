package org.texttechnologylab.duui.pipeline.io;

import org.apache.uima.cas.SerialFormat;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.pipeline.DUUITarget;
import org.texttechnologylab.duui.runtime.DUUIFlowScope;
import org.texttechnologylab.duui.runtime.DUUITargetScope;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class DUUIXmiTarget implements DUUITarget<JCas> {
    private final Path outputDirectory;

    private DUUIXmiTarget(Builder builder) {
        this.outputDirectory = Objects.requireNonNull(builder.outputDirectory, "outputDirectory");
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void accept(DUUIArtifact<JCas> artifact) throws Exception {
        Files.createDirectories(outputDirectory);
        Path output = outputDirectory.resolve(artifact.id() + ".xmi");
        try (OutputStream stream = Files.newOutputStream(output)) {
            CasIOUtils.save(artifact.payload().getCas(), stream, SerialFormat.XMI_1_1_PRETTY);
        }
    }

    public DUUITargetScope<JCas> open(DUUIFlowScope<JCas> parent) {
        return parent.pipeline().target(parent, this);
    }

    public static final class Builder {
        private Path outputDirectory;

        private Builder() {
        }

        public Builder output(Path outputDirectory) {
            this.outputDirectory = Objects.requireNonNull(outputDirectory, "outputDirectory");
            return this;
        }

        public DUUIXmiTarget build() {
            return new DUUIXmiTarget(this);
        }

        public DUUITargetScope<JCas> open(DUUIFlowScope<JCas> parent) {
            return build().open(parent);
        }
    }
}
