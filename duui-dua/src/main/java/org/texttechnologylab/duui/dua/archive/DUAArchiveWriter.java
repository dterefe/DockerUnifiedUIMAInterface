package org.texttechnologylab.duui.dua.archive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.texttechnologylab.duui.dua.DUA;
import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.distributed.DUADistributionPlan;
import org.texttechnologylab.duui.dua.distributed.DUAShardManifest;
import org.texttechnologylab.duui.dua.distributed.DUAShardObjectRef;
import org.texttechnologylab.duui.dua.distributed.DUAShardReplica;
import org.texttechnologylab.duui.dua.graph.DUAGraphCodec;
import org.texttechnologylab.duui.dua.graph.DUAGraphPartition;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
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

    public DUAArchiveWriter addPartition(DUAGraphPartition partition, DUAGraphCodec codec) throws IOException {
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(codec, "codec");
        String directory = DUA.GRAPHS + sanitize(partition.id()) + "/" + codec.id() + "/";
        String path = directory + codec.defaultFileName();
        Path target = staging.resolve(path);
        codec.write(partition, target);
        manifest.getPartitions().add(new DUAPartitionEntry(partition.id(), codec.id(), path, partition.scope()));
        return this;
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

    public DUAArchiveWriter addDistributionPlan(DUADistributionPlan plan) throws IOException {
        Objects.requireNonNull(plan, "plan");
        String routingPath = DUA.DISTRIBUTION + "routing/"
                + sanitize(plan.routingTable().routingTableId()) + ".json";
        writeJson(routingPath, plan.routingTable());
        for (DUAShardManifest shard : plan.shards()) {
            String shardPath = DUA.PARTITIONS + sanitize(shard.partitionId())
                    + "/shards/" + sanitize(shard.shardId()) + "/manifest.json";
            writeJson(shardPath, shard);
        }
        return this;
    }

    public DUAArchiveWriter addCasShardDirectory(String partitionId,
                                                 String shardId,
                                                 Path storageDirectory,
                                                 long rangeStart,
                                                 long rangeEndExclusive,
                                                 List<DUAShardReplica> replicas) throws IOException {
        Objects.requireNonNull(partitionId, "partitionId");
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(storageDirectory, "storageDirectory");
        String shardRoot = DUA.PARTITIONS + sanitize(partitionId)
                + "/shards/" + sanitize(shardId) + "/";
        String objectRoot = shardRoot + "objects/";
        List<DUAShardObjectRef> objects = new ArrayList<>();
        try (var paths = Files.walk(storageDirectory)) {
            for (Path source : paths.filter(Files::isRegularFile).sorted().toList()) {
                String relative = storageDirectory.relativize(source).toString().replace('\\', '/');
                String objectPath = objectRoot + relative;
                Path target = staging.resolve(objectPath);
                Files.createDirectories(target.getParent());
                Files.copy(source, target);
                objects.add(new DUAShardObjectRef(objectPath, sha256(target), Files.size(target)));
            }
        }
        writeJson(shardRoot + "manifest.json", new DUAShardManifest(
                shardId,
                partitionId,
                0,
                rangeStart,
                rangeEndExclusive,
                objects,
                replicas));
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
        Files.createDirectories(staging.resolve(DUA.GRAPHS));
        Files.createDirectories(staging.resolve(DUA.TYPESYSTEMS));
        Files.createDirectories(staging.resolve(DUA.CAS));
        Files.createDirectories(staging.resolve(DUA.INDEXES));
        Files.createDirectories(staging.resolve(DUA.DISTRIBUTION));
        Files.createDirectories(staging.resolve(DUA.PARTITIONS));
        Files.createDirectories(staging.resolve(DUA.SCHEMAS));
    }

    private void writeJson(String path, Object value) throws IOException {
        Path target = staging.resolve(path);
        Files.createDirectories(target.getParent());
        MAPPER.writeValue(target.toFile(), value);
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

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    digest.update(buffer, 0, read);
                }
            }
            StringBuilder builder = new StringBuilder(64);
            for (byte b : digest.digest()) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
