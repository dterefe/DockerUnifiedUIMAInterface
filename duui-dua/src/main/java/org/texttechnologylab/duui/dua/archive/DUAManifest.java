package org.texttechnologylab.duui.dua.archive;

import org.texttechnologylab.duui.dua.DUA;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.texttechnologylab.duui.dua.backend.DUABackendLayout;

public final class DUAManifest {
    private String format = DUA.FORMAT;
    private int version = DUA.FORMAT_VERSION;
    private String universeId;
    private Instant createdAt = Instant.now();
    private int nextFsId = 1;
    private DUABackendLayout backendLayout = DUABackendLayout.inMemory();
    private List<DUAArtifactEntry> artifacts = new ArrayList<>();
    private List<DUAStoreSnapshotEntry> storeSnapshots = new ArrayList<>();

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

    public int getNextFsId() {
        return nextFsId;
    }

    public void setNextFsId(int nextFsId) {
        this.nextFsId = nextFsId;
    }

    public DUABackendLayout getBackendLayout() {
        return backendLayout;
    }

    public void setBackendLayout(DUABackendLayout backendLayout) {
        this.backendLayout = backendLayout == null ? DUABackendLayout.inMemory() : backendLayout;
    }

    public List<DUAArtifactEntry> getArtifacts() {
        return artifacts;
    }

    public void setArtifacts(List<DUAArtifactEntry> artifacts) {
        this.artifacts = artifacts == null ? new ArrayList<>() : artifacts;
    }

    public List<DUAStoreSnapshotEntry> getStoreSnapshots() {
        return storeSnapshots;
    }

    public void setStoreSnapshots(List<DUAStoreSnapshotEntry> storeSnapshots) {
        this.storeSnapshots = storeSnapshots == null ? new ArrayList<>() : storeSnapshots;
    }
}
