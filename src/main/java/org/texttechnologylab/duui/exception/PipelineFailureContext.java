package org.texttechnologylab.duui.exception;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class PipelineFailureContext {
    private final String importId;
    private final Long corpusId;
    private final Long documentDbId;
    private final String documentId;
    private final String sourcePath;
    private final String stageName;
    private final String dependencyName;
    private final Map<String, String> attributes;

    private PipelineFailureContext(Builder builder) {
        this.importId = builder.importId;
        this.corpusId = builder.corpusId;
        this.documentDbId = builder.documentDbId;
        this.documentId = builder.documentId;
        this.sourcePath = builder.sourcePath;
        this.stageName = builder.stageName;
        this.dependencyName = builder.dependencyName;
        this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(builder.attributes));
    }

    public static Builder builder() { return new Builder(); }

    public String getImportId() { return importId; }
    public Long getCorpusId() { return corpusId; }
    public Long getDocumentDbId() { return documentDbId; }
    public String getDocumentId() { return documentId; }
    public String getSourcePath() { return sourcePath; }
    public String getStageName() { return stageName; }
    public String getDependencyName() { return dependencyName; }
    public Map<String, String> getAttributes() { return attributes; }

    public static final class Builder {
        private String importId;
        private Long corpusId;
        private Long documentDbId;
        private String documentId;
        private String sourcePath;
        private String stageName;
        private String dependencyName;
        private final Map<String, String> attributes = new LinkedHashMap<>();

        public Builder importId(String importId) { this.importId = importId; return this; }
        public Builder corpusId(Long corpusId) { this.corpusId = corpusId; return this; }
        public Builder documentDbId(Long documentDbId) { this.documentDbId = documentDbId; return this; }
        public Builder documentId(String documentId) { this.documentId = documentId; return this; }
        public Builder sourcePath(String sourcePath) { this.sourcePath = sourcePath; return this; }
        public Builder stageName(String stageName) { this.stageName = stageName; return this; }
        public Builder dependencyName(String dependencyName) { this.dependencyName = dependencyName; return this; }
        public Builder attribute(String key, String value) {
            if (key != null && value != null) attributes.put(key, value);
            return this;
        }
        public PipelineFailureContext build() { return new PipelineFailureContext(this); }
    }
}
