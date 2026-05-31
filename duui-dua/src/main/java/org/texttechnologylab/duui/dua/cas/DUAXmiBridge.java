package org.texttechnologylab.duui.dua.cas;

import org.apache.uima.cas.SerialFormat;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.texttechnologylab.duui.dua.archive.DUAArchiveReader;
import org.texttechnologylab.duui.dua.archive.DUAArchiveWriter;
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
    private final DUAXmiFsIdRemapper fsIdRemapper = new DUAXmiFsIdRemapper();

    public void importDirectory(Path source, Path dua) throws Exception {
        Objects.requireNonNull(source, "source");
        try (DUAArchiveWriter writer = DUAArchiveWriter.create(dua)) {
            try (Stream<Path> paths = Files.list(source)) {
                for (Path path : paths
                        .filter(Files::isRegularFile)
                        .filter(candidate -> candidate.getFileName().toString().endsWith(".xmi"))
                        .sorted(Comparator.comparing(Path::toString))
                        .toList()) {
                    importXmi(path, writer);
                }
            }
        }
    }

    public void importXmi(Path source, DUAArchiveWriter writer) throws Exception {
        String documentId = documentId(source);
        JCas view = JCasFactory.createJCas();
        byte[] sourcePayload = Files.readAllBytes(source);
        try (var input = Files.newInputStream(source)) {
            CasIOUtils.load(input, view.getCas());
        }
        writer.addArtifact(documentId, "cas-xmi", "application/vnd.apache.uima.cas+xmi",
                fsIdRemapper.remap(sourcePayload, view, writer::allocateFsId));
    }

    public String addCasPayload(JCas view, DUAArchiveWriter writer, String documentId) throws Exception {
        String effectiveDocumentId = documentId == null ? DUAId.create().value() : documentId;
        writer.addArtifact(effectiveDocumentId, "cas-xmi", "application/vnd.apache.uima.cas+xmi",
                fsIdRemapper.remap(serialize(view), view, writer::allocateFsId));
        return effectiveDocumentId;
    }

    public JCas materialize(DUAArchiveReader reader, String documentId) throws Exception {
        JCas view = JCasFactory.createJCas();
        try (var input = new ByteArrayInputStream(reader.artifactPayload(documentId))) {
            CasIOUtils.load(input, view.getCas());
        }
        return view;
    }

    public void exportXmi(DUAArchiveReader reader, String documentId, Path target) throws IOException {
        Path parent = target.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(target, reader.artifactPayload(documentId));
    }

    private static byte[] serialize(JCas view) throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            CasIOUtils.save(view.getCas(), output, SerialFormat.XMI_1_1);
            return output.toByteArray();
        }
    }

    private static String documentId(Path source) {
        String fileName = source.getFileName().toString();
        return fileName.endsWith(".xmi") ? fileName.substring(0, fileName.length() - 4) : fileName;
    }
}
