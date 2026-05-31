package org.texttechnologylab.duui.pipeline.io.document;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasIOUtils;
import org.texttechnologylab.duui.filesystem.DUUIFile;

import java.io.InputStream;

public final class DUUIXmiDocumentDeserializer implements DUUIDocumentDeserializer<JCas> {
    private final TypeSystemDescription typeSystemDescription;

    public DUUIXmiDocumentDeserializer() {
        this(null);
    }

    public DUUIXmiDocumentDeserializer(TypeSystemDescription typeSystemDescription) {
        this.typeSystemDescription = typeSystemDescription;
    }

    @Override
    public JCas read(DUUIFile source, InputStream input) throws Exception {
        JCas cas = createCas();
        try (InputStream decoded = decoded(source.name(), input)) {
            CasIOUtils.load(decoded, cas.getCas());
        }
        return cas;
    }

    private JCas createCas() throws ResourceInitializationException, CASException {
        if (typeSystemDescription == null) {
            return JCasFactory.createJCas();
        }
        return JCasFactory.createJCas(typeSystemDescription);
    }

    private InputStream decoded(String fileName, InputStream input) throws Exception {
        if (fileName.endsWith(".xz")) {
            return new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.XZ, input);
        }
        if (fileName.endsWith(".gz")) {
            return new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, input);
        }
        if (fileName.endsWith(".bz2")) {
            return new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2, input);
        }
        return input;
    }
}
