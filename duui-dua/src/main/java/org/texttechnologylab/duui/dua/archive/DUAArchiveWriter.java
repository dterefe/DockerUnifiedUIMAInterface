package org.texttechnologylab.duui.dua.archive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.texttechnologylab.duui.dua.DUA;
import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.backend.DUABackendLayout;
import org.texttechnologylab.duui.dua.backend.DUAStoreRole;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public final class DUAArchiveWriter implements Closeable {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.INDENT_OUTPUT);

    private final Path output;
    private final Path staging;
    private final DUAManifest manifest;
    private boolean closed;

    private DUAArchiveWriter(Path output, String universeId) throws IOException {
        this.output = Objects.requireNonNull(output, "output");
        this.staging = Files.createTempDirectory("dua-writer-");
        this.manifest = new DUAManifest();
        this.manifest.setUniverseId(universeId == null ? DUAId.create().value() : universeId);
        createLayout();
    }

    public static DUAArchiveWriter create(Path output) throws IOException {
        return new DUAArchiveWriter(output, null);
    }

    public static DUAArchiveWriter create(Path output, String universeId) throws IOException {
        return new DUAArchiveWriter(output, universeId);
    }

    public DUAManifest manifest() {
        return manifest;
    }

    public DUAArchiveWriter backendLayout(DUABackendLayout layout) {
        manifest.setBackendLayout(layout);
        return this;
    }

    public synchronized int allocateFsId() {
        int allocated = manifest.getNextFsId();
        if (allocated < 1 || allocated == Integer.MAX_VALUE) {
            throw new DUAArchiveException("DUA archive fs id space is exhausted");
        }
        manifest.setNextFsId(allocated + 1);
        return allocated;
    }

    public DUAArchiveWriter addArtifact(String id, String kind, String mediaType, byte[] payload) throws IOException {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(payload, "payload");
        String path = DUA.ARTIFACTS + sanitize(kind) + "/" + sanitize(id) + ".bin";
        Path target = staging.resolve(path);
        Files.createDirectories(target.getParent());
        Files.write(target, payload);
        manifest.getArtifacts().add(new DUAArtifactEntry(id, kind, path, mediaType == null ? "application/octet-stream" : mediaType));
        return this;
    }

    public DUAArchiveWriter addStoreSnapshot(DUAStoreRole role, String id, String mediaType, byte[] payload)
            throws IOException {
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(payload, "payload");
        String path = DUA.STORES + sanitize(role.name().toLowerCase()) + "/" + sanitize(id) + ".bin";
        Path target = staging.resolve(path);
        Files.createDirectories(target.getParent());
        Files.write(target, payload);
        manifest.getStoreSnapshots().add(new DUAStoreSnapshotEntry(
                id, role, path, mediaType == null ? "application/octet-stream" : mediaType));
        return this;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        MAPPER.writeValue(staging.resolve(DUA.MANIFEST).toFile(), manifest);
        Path parent = output.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (OutputStream file = Files.newOutputStream(output);
             ZipOutputStream zip = new ZipOutputStream(file)) {
            try (var paths = Files.walk(staging)) {
                for (Path path : paths.filter(Files::isRegularFile).sorted().toList()) {
                    String entryName = staging.relativize(path).toString().replace('\\', '/');
                    zip.putNextEntry(new ZipEntry(entryName));
                    Files.copy(path, zip);
                    zip.closeEntry();
                }
            }
        } finally {
            deleteStaging();
        }
    }

    private void createLayout() throws IOException {
        Files.createDirectories(staging.resolve(DUA.ARTIFACTS));
        Files.createDirectories(staging.resolve(DUA.TYPESYSTEMS));
        Files.createDirectories(staging.resolve(DUA.CAS));
        Files.createDirectories(staging.resolve(DUA.INDEXES));
        Files.createDirectories(staging.resolve(DUA.STORES));
    }

    private void deleteStaging() throws IOException {
        if (!Files.exists(staging)) {
            return;
        }
        try (var paths = Files.walk(staging)) {
            for (Path path : paths.sorted((left, right) -> right.compareTo(left)).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static String sanitize(String value) {
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}
