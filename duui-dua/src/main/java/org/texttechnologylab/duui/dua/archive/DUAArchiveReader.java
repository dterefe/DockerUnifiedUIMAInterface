package org.texttechnologylab.duui.dua.archive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.texttechnologylab.duui.dua.DUA;
import org.texttechnologylab.duui.dua.graph.DUAGraphCodec;
import org.texttechnologylab.duui.dua.graph.DUAGraphCodecs;
import org.texttechnologylab.duui.dua.graph.DUAGraphPartition;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipFile;

public final class DUAArchiveReader implements Closeable {
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final Path archive;
    private final Path staging;
    private final DUAManifest manifest;
    private final DUAGraphCodecs codecs;

    private DUAArchiveReader(Path archive, DUAGraphCodecs codecs) throws IOException {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.codecs = codecs == null ? DUAGraphCodecs.defaults() : codecs;
        this.staging = Files.createTempDirectory("dua-reader-");
        unzip();
        this.manifest = MAPPER.readValue(staging.resolve(DUA.MANIFEST).toFile(), DUAManifest.class);
        validateManifest();
    }

    public static DUAArchiveReader open(Path archive) throws IOException {
        return new DUAArchiveReader(archive, DUAGraphCodecs.defaults());
    }

    public static DUAArchiveReader open(Path archive, DUAGraphCodecs codecs) throws IOException {
        return new DUAArchiveReader(archive, codecs);
    }

    public DUAManifest manifest() {
        return manifest;
    }

    public List<DUAPartitionEntry> partitions() {
        return List.copyOf(manifest.getPartitions());
    }

    public Optional<DUAGraphPartition> partition(String id, String codecId) throws IOException {
        for (DUAPartitionEntry entry : manifest.getPartitions()) {
            if (entry.id().equals(id) && entry.codec().equals(codecId)) {
                DUAGraphCodec codec = codecs.require(codecId);
                return Optional.of(codec.read(entry.id(), entry.scope(), staging.resolve(entry.path())));
            }
        }
        return Optional.empty();
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
    }
}
