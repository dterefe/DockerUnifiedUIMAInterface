package org.texttechnologylab.duui.artifact;

import java.time.Instant;

public final class DUUIArtifactContext {
    private final String importId;
    /**
     * Hierarchical owner artifact, e.g. a UCEDocument artifact belongs to a UCECorpus artifact.
     */
    private final String parentArtifactId;
    /**
     * Previous artifact in the same logical branch, e.g. a UCEDocument artifact becoming a JCas artifact.
     */
    private final String predecessorArtifactId;
    /**
     * External or original provenance artifact/source id. This is not necessarily the direct predecessor.
     */
    private final String sourceArtifactId;
    private final String sourcePath;
    private final Instant createdAt;

    private DUUIArtifactContext(Builder builder) {
        this.importId = builder.importId;
        this.parentArtifactId = builder.parentArtifactId;
        this.predecessorArtifactId = builder.predecessorArtifactId;
        this.sourceArtifactId = builder.sourceArtifactId;
        this.sourcePath = builder.sourcePath;
        this.createdAt = builder.createdAt == null ? Instant.now() : builder.createdAt;
    }

    public static DUUIArtifactContext empty() { return builder().build(); }
    public static Builder builder() { return new Builder(); }
    public Builder toBuilder() {
        return builder()
                .importId(importId)
                .parentArtifactId(parentArtifactId)
                .predecessorArtifactId(predecessorArtifactId)
                .sourceArtifactId(sourceArtifactId)
                .sourcePath(sourcePath)
                .createdAt(createdAt);
    }

    public String importId() { return importId; }
    public String parentArtifactId() { return parentArtifactId; }
    public String predecessorArtifactId() { return predecessorArtifactId; }
    public String sourceArtifactId() { return sourceArtifactId; }
    public String sourcePath() { return sourcePath; }
    public Instant createdAt() { return createdAt; }

    public static final class Builder {
        private String importId;
        private String parentArtifactId;
        private String predecessorArtifactId;
        private String sourceArtifactId;
        private String sourcePath;
        private Instant createdAt;

        public Builder importId(String importId) { this.importId = importId; return this; }
        public Builder parentArtifactId(String parentArtifactId) { this.parentArtifactId = parentArtifactId; return this; }
        public Builder predecessorArtifactId(String predecessorArtifactId) { this.predecessorArtifactId = predecessorArtifactId; return this; }
        public Builder sourceArtifactId(String sourceArtifactId) { this.sourceArtifactId = sourceArtifactId; return this; }
        public Builder sourcePath(String sourcePath) { this.sourcePath = sourcePath; return this; }
        public Builder createdAt(Instant createdAt) { this.createdAt = createdAt; return this; }
        public DUUIArtifactContext build() { return new DUUIArtifactContext(this); }
    }
}
