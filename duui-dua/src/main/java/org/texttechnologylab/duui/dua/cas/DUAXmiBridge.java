package org.texttechnologylab.duui.dua.cas;

import org.apache.uima.cas.SerialFormat;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.texttechnologylab.duui.dua.archive.DUAArchiveReader;
import org.texttechnologylab.duui.dua.archive.DUAArchiveWriter;
import org.texttechnologylab.duui.dua.graph.DUAGraphCodec;
import org.texttechnologylab.duui.dua.DUAId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

public final class DUAXmiBridge {
    private final DUACasGraph casGraph = new DUACasGraph();

    public void importDirectory(Path source, Path dua, String corpusId, DUAGraphCodec codec) throws Exception {
        Objects.requireNonNull(source, "source");
        try (DUAArchiveWriter writer = DUAArchiveWriter.create(dua)) {
            try (Stream<Path> paths = Files.list(source)) {
                for (Path path : paths
                        .filter(Files::isRegularFile)
                        .filter(candidate -> candidate.getFileName().toString().endsWith(".xmi"))
                        .sorted(Comparator.comparing(Path::toString))
                        .toList()) {
                    importXmi(path, writer, corpusId, codec);
                }
            }
        }
    }

    public void importXmi(Path source, DUAArchiveWriter writer, String corpusId, DUAGraphCodec codec) throws Exception {
        String documentId = documentId(source);
        JCas jCas = JCasFactory.createJCas();
        try (var input = Files.newInputStream(source)) {
            CasIOUtils.load(input, jCas.getCas());
        }
        writer.addArtifact(documentId, "cas-xmi", "application/vnd.apache.uima.cas+xmi", Files.readAllBytes(source));
        writer.addPartition(casGraph.fromCas(corpusId, documentId, jCas.getCas()), codec);
    }

    public String addJCas(JCas jCas, DUAArchiveWriter writer, String corpusId, String documentId, DUAGraphCodec codec) throws Exception {
        String effectiveDocumentId = documentId == null ? DUAId.create().value() : documentId;
        writer.addArtifact(effectiveDocumentId, "cas-xmi", "application/vnd.apache.uima.cas+xmi", serialize(jCas));
        writer.addPartition(casGraph.fromCas(corpusId, effectiveDocumentId, jCas.getCas()), codec);
        return effectiveDocumentId;
    }

    public JCas materialize(DUAArchiveReader reader, String documentId) throws Exception {
        JCas jCas = JCasFactory.createJCas();
        try (var input = new ByteArrayInputStream(reader.artifactPayload(documentId))) {
            CasIOUtils.load(input, jCas.getCas());
        }
        return jCas;
    }

    public void exportXmi(DUAArchiveReader reader, String documentId, Path target) throws IOException {
        Path parent = target.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(target, reader.artifactPayload(documentId));
    }

    private static byte[] serialize(JCas jCas) throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            CasIOUtils.save(jCas.getCas(), output, SerialFormat.XMI_1_1);
            return output.toByteArray();
        }
    }

    private static String documentId(Path source) {
        String fileName = source.getFileName().toString();
        return fileName.endsWith(".xmi") ? fileName.substring(0, fileName.length() - 4) : fileName;
    }
}
