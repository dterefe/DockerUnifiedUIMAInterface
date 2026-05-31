package org.texttechnologylab.duui.pipeline.io.document;

import org.apache.uima.cas.SerialFormat;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.texttechnologylab.duui.artifact.DUUIArtifact;

import java.io.OutputStream;
import java.util.Objects;

public final class DUUIXmiDocumentSerializer implements DUUIDocumentSerializer<JCas> {
    private final SerialFormat format;

    public DUUIXmiDocumentSerializer() {
        this(SerialFormat.XMI_1_1_PRETTY);
    }

    public DUUIXmiDocumentSerializer(SerialFormat format) {
        this.format = Objects.requireNonNull(format, "format");
    }

    @Override
    public void write(DUUIArtifact<JCas> artifact, OutputStream output) throws Exception {
        CasIOUtils.save(artifact.payload().getCas(), output, format);
    }
}
