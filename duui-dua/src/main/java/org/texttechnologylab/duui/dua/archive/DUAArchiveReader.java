package org.texttechnologylab.duui.dua.archive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.texttechnologylab.duui.dua.DUA;
import org.texttechnologylab.duui.dua.backend.DUAStoreRole;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.zip.ZipFile;

public final class DUAArchiveReader implements Closeable {
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final Path archive;
    private final Path staging;
    private final DUAManifest manifest;

    private DUAArchiveReader(Path archive) throws IOException {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.staging = Files.createTempDirectory("dua-reader-");
        unzip();
        this.manifest = MAPPER.readValue(staging.resolve(DUA.MANIFEST).toFile(), DUAManifest.class);
        validateManifest();
    }

    public static DUAArchiveReader open(Path archive) throws IOException {
        return new DUAArchiveReader(archive);
    }

    public DUAManifest manifest() {
        return manifest;
    }

    public byte[] artifactPayload(String id) throws IOException {
        for (DUAArtifactEntry entry : manifest.getArtifacts()) {
            if (entry.id().equals(id)) {
                return Files.readAllBytes(staging.resolve(entry.path()));
            }
        }
        throw new DUAArchiveException("No DUA artifact payload for id " + id);
    }

    public byte[] resourcePayload(String path) throws IOException {
        Path target = staging.resolve(path).normalize();
        if (!target.startsWith(staging)) {
            throw new DUAArchiveException("Illegal DUA resource path: " + path);
        }
        return Files.readAllBytes(target);
    }

    public byte[] storeSnapshotPayload(DUAStoreRole role, String id) throws IOException {
        for (DUAStoreSnapshotEntry entry : manifest.getStoreSnapshots()) {
            if (entry.role() == role && entry.id().equals(id)) {
                return resourcePayload(entry.path());
            }
        }
        throw new DUAArchiveException("No DUA store snapshot for role " + role + " and id " + id);
    }

    @Override
    public void close() throws IOException {
        try (var paths = Files.walk(staging)) {
            for (Path path : paths.sorted((left, right) -> right.compareTo(left)).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private void unzip() throws IOException {
        try (ZipFile zip = new ZipFile(archive.toFile())) {
            var entries = zip.entries();
            while (entries.hasMoreElements()) {
                var entry = entries.nextElement();
                Path target = staging.resolve(entry.getName()).normalize();
                if (!target.startsWith(staging)) {
                    throw new DUAArchiveException("Illegal DUA ZIP entry path: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    try (var input = zip.getInputStream(entry)) {
                        Files.copy(input, target);
                    }
                }
            }
        }
    }

    private void validateManifest() {
        if (!DUA.FORMAT.equals(manifest.getFormat())) {
            throw new DUAArchiveException("Unsupported archive format: " + manifest.getFormat());
        }
        if (manifest.getVersion() != DUA.FORMAT_VERSION) {
            throw new DUAArchiveException("Unsupported DUA version: " + manifest.getVersion());
        }
        if (manifest.getBackendLayout() == null || manifest.getBackendLayout().getStores().isEmpty()) {
            throw new DUAArchiveException("DUA archive has no backend layout");
        }
    }
}
