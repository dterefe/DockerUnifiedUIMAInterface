package org.texttechnologylab.duui.dua.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Registry that maps virtual corpus IDs to their member document IDs.
 * This is an index, not a query engine — it tracks which documents
 * belong to which virtual corpora discovered during JCas import.
 *
 * <p>Persisted as {@code indexes/virtual_corpora.json} inside the
 * {@code .dua} archive.</p>
 */
public final class VirtualCorpusRegistry {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private final Map<String, Set<String>> corpusToDocuments;

    public VirtualCorpusRegistry() {
        this.corpusToDocuments = new LinkedHashMap<>();
    }

    @JsonCreator
    private VirtualCorpusRegistry(
            @JsonProperty("corpusToDocuments") Map<String, Set<String>> corpusToDocuments) {
        this.corpusToDocuments = corpusToDocuments == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(corpusToDocuments);
    }

    /**
     * Registers a new virtual corpus. If the corpus already exists, this is a no-op.
     *
     * @param corpusId the ID of the corpus to register
     * @return true if the corpus was newly created, false if it already existed
     */
    public boolean registerCorpus(String corpusId) {
        Objects.requireNonNull(corpusId, "corpusId");
        if (corpusToDocuments.containsKey(corpusId)) {
            return false;
        }
        corpusToDocuments.put(corpusId, new LinkedHashSet<>());
        return true;
    }

    /**
     * Assigns a document to a virtual corpus. Creates the corpus if it does not exist yet.
     *
     * @param corpusId   the ID of the corpus
     * @param documentId the ID of the document
     * @return true if the document was newly added to this corpus, false if it was already assigned
     */
    public boolean assignDocument(String corpusId, String documentId) {
        Objects.requireNonNull(corpusId, "corpusId");
        Objects.requireNonNull(documentId, "documentId");
        Set<String> docs = corpusToDocuments.computeIfAbsent(corpusId, k -> new LinkedHashSet<>());
        return docs.add(documentId);
    }

    /**
     * Returns an unmodifiable view of the document IDs in a virtual corpus.
     *
     * @param corpusId the ID of the corpus
     * @return the set of document IDs, or an empty set if the corpus is not registered
     */
    public Set<String> getDocuments(String corpusId) {
        Set<String> docs = corpusToDocuments.get(corpusId);
        return docs == null ? Collections.emptySet() : Collections.unmodifiableSet(docs);
    }

    /**
     * Checks whether a virtual corpus with the given ID is registered.
     *
     * @param corpusId the ID of the corpus
     * @return true if the corpus exists in the registry
     */
    public boolean hasCorpus(String corpusId) {
        return corpusToDocuments.containsKey(corpusId);
    }

    /**
     * Returns an unmodifiable view of all registered corpus IDs.
     *
     * @return the set of corpus IDs
     */
    public Set<String> corpusIds() {
        return Collections.unmodifiableSet(corpusToDocuments.keySet());
    }

    /**
     * Returns the number of registered virtual corpora.
     *
     * @return the corpus count
     */
    public int corpusCount() {
        return corpusToDocuments.size();
    }

    /**
     * Returns the total number of document-to-corpus assignments.
     *
     * @return the total assignment count
     */
    public int assignmentCount() {
        int count = 0;
        for (Set<String> docs : corpusToDocuments.values()) {
            count += docs.size();
        }
        return count;
    }

    public boolean isEmpty() {
        return corpusToDocuments.isEmpty();
    }

    // -- serialization ---------------------------------------------------------

    /**
     * Serializes this registry as JSON to the given output stream.
     */
    public void writeJson(OutputStream output) throws IOException {
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(output, this);
    }

    /**
     * Deserializes a {@code VirtualCorpusRegistry} from JSON in the given input stream.
     */
    public static VirtualCorpusRegistry readJson(InputStream input) throws IOException {
        return MAPPER.readValue(input, VirtualCorpusRegistry.class);
    }

    // -- JSON property accessors -----------------------------------------------

    @JsonProperty("corpusToDocuments")
    Map<String, Set<String>> corpusToDocuments() {
        return corpusToDocuments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VirtualCorpusRegistry that)) return false;
        return corpusToDocuments.equals(that.corpusToDocuments);
    }

    @Override
    public int hashCode() {
        return corpusToDocuments.hashCode();
    }

    @Override
    public String toString() {
        return "VirtualCorpusRegistry{corpora=" + corpusToDocuments.size()
                + ", assignments=" + assignmentCount() + "}";
    }
}
