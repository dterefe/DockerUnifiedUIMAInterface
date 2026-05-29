package org.texttechnologylab.duui.dua.archive;

import org.texttechnologylab.duui.dua.DUA;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class DUAManifest {
    private String format = DUA.FORMAT;
    private int version = DUA.FORMAT_VERSION;
    private String universeId;
    private Instant createdAt = Instant.now();
    private List<DUAPartitionEntry> partitions = new ArrayList<>();
    private List<DUAArtifactEntry> artifacts = new ArrayList<>();

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getUniverseId() {
        return universeId;
    }

    public void setUniverseId(String universeId) {
        this.universeId = universeId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public List<DUAPartitionEntry> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<DUAPartitionEntry> partitions) {
        this.partitions = partitions == null ? new ArrayList<>() : partitions;
    }

    public List<DUAArtifactEntry> getArtifacts() {
        return artifacts;
    }

    public void setArtifacts(List<DUAArtifactEntry> artifacts) {
        this.artifacts = artifacts == null ? new ArrayList<>() : artifacts;
    }
}
