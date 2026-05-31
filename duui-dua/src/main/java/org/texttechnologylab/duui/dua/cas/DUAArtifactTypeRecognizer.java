package org.texttechnologylab.duui.dua.cas;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;
import org.texttechnologylab.duui.dua.store.VirtualCorpusRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Recognizes domain Artifact types ({@code Corpus}, {@code Document},
 * {@code Membership}) in a UIMA CAS using generic type-system introspection.
 *
 * <p>duui-dua does not depend on UIMATypeSystem, so type checks are
 * performed by type name and feature presence rather than concrete
 * JCas classes.</p>
 *
 * <p>The set of recognized type names is configurable via
 * {@link #addCorpusType(String)} and friends, making the recognizer
 * extensible to alternative type-system layouts.</p>
 */
public final class DUAArtifactTypeRecognizer {

    private final Set<String> corpusTypeNames = new LinkedHashSet<>();
    private final Set<String> documentTypeNames = new LinkedHashSet<>();
    private final Set<String> membershipTypeNames = new LinkedHashSet<>();

    // Feature short names used for identification and value extraction.
    private static final String FEATURE_ID = "id";
    private static final String FEATURE_DOCUMENT_IDS = "documentIds";
    private static final String FEATURE_WHOLE = "whole";
    private static final String FEATURE_PART = "part";

    /**
     * Creates a recognizer pre-configured with the canonical
     * {@code org.texttechnologylab.annotation.artifact.*} and
     * {@code org.texttechnologylab.annotation.domain.*} type names.
     */
    public static DUAArtifactTypeRecognizer createDefault() {
        DUAArtifactTypeRecognizer recognizer = new DUAArtifactTypeRecognizer();
        recognizer.addCorpusType("org.texttechnologylab.annotation.artifact.Corpus");
        recognizer.addDocumentType("org.texttechnologylab.annotation.artifact.Document");
        recognizer.addMembershipType("org.texttechnologylab.annotation.domain.Membership");
        return recognizer;
    }

    public void addCorpusType(String typeName) {
        corpusTypeNames.add(Objects.requireNonNull(typeName, "typeName"));
    }

    public void addDocumentType(String typeName) {
        documentTypeNames.add(Objects.requireNonNull(typeName, "typeName"));
    }

    public void addMembershipType(String typeName) {
        membershipTypeNames.add(Objects.requireNonNull(typeName, "typeName"));
    }

    /**
     * Scans the given {@code CAS} for recognized Corpus and Document annotations
     * and populates the {@code registry} accordingly.
     *
     * <p>For every Corpus annotation found, extracts its {@code id} and
     * {@code documentIds} features and registers/assigns them. Memberships
     * are also resolved if present.</p>
     *
     * @param cas      the CAS to scan
     * @param documentId the identifier of the document being imported
     * @param registry the registry to populate
     * @return the number of corpus annotations discovered
     */
    public int recognizeAndAssign(CAS cas, String documentId, VirtualCorpusRegistry registry) {
        Objects.requireNonNull(cas, "cas");
        Objects.requireNonNull(documentId, "documentId");
        Objects.requireNonNull(registry, "registry");

        TypeSystem ts = cas.getTypeSystem();
        int found = 0;

        // 1. Scan for Corpus annotations
        for (String corpusTypeName : corpusTypeNames) {
            Type corpusType = ts.getType(corpusTypeName);
            if (corpusType == null) {
                continue;
            }
            var iterator = cas.getIndexRepository().getAllIndexedFS(corpusType);
            while (iterator.hasNext()) {
                FeatureStructure fs = iterator.next();
                String id = getStringFeature(fs, FEATURE_ID);
                if (id == null || id.isEmpty()) {
                    continue;
                }
                registry.registerCorpus(id);
                found++;

                // Extract documentIds (StringArray-like feature)
                Feature docIdsFeature = fs.getType().getFeatureByBaseName(FEATURE_DOCUMENT_IDS);
                if (docIdsFeature != null) {
                    FeatureStructure arrayVal = fs.getFeatureValue(docIdsFeature);
                    if (arrayVal instanceof org.apache.uima.cas.StringArray stringArray) {
                        for (int i = 0; i < stringArray.size(); i++) {
                            String docId = stringArray.get(i);
                            if (docId != null && !docId.isEmpty()) {
                                registry.assignDocument(id, docId);
                            }
                        }
                    }
                }
                // Also assign the current document to this corpus
                registry.assignDocument(id, documentId);
            }
        }

        // 2. Scan for Membership associations
        for (String membershipTypeName : membershipTypeNames) {
            Type membershipType = ts.getType(membershipTypeName);
            if (membershipType == null) {
                continue;
            }
            var iterator = cas.getIndexRepository().getAllIndexedFS(membershipType);
            while (iterator.hasNext()) {
                FeatureStructure fs = iterator.next();
                FeatureStructure whole = getFeatureValue(fs, FEATURE_WHOLE);
                FeatureStructure part = getFeatureValue(fs, FEATURE_PART);
                if (whole != null && part != null) {
                    String wholeId = getStringFeature(whole, FEATURE_ID);
                    String partId = getStringFeature(part, FEATURE_ID);
                    if (wholeId != null && partId != null) {
                        registry.registerCorpus(wholeId);
                        registry.assignDocument(wholeId, partId);
                    }
                }
            }
        }

        return found;
    }

    /**
     * Lists the corpus IDs discovered from a CAS without modifying any registry.
     */
    public List<String> discoverCorpusIds(CAS cas) {
        Objects.requireNonNull(cas, "cas");
        TypeSystem ts = cas.getTypeSystem();
        List<String> ids = new ArrayList<>();
        for (String corpusTypeName : corpusTypeNames) {
            Type corpusType = ts.getType(corpusTypeName);
            if (corpusType == null) {
                continue;
            }
            var iterator = cas.getIndexRepository().getAllIndexedFS(corpusType);
            while (iterator.hasNext()) {
                FeatureStructure fs = iterator.next();
                String id = getStringFeature(fs, FEATURE_ID);
                if (id != null && !id.isEmpty()) {
                    ids.add(id);
                }
            }
        }
        return ids;
    }

    public Set<String> corpusTypeNames() {
        return Collections.unmodifiableSet(corpusTypeNames);
    }

    public Set<String> documentTypeNames() {
        return Collections.unmodifiableSet(documentTypeNames);
    }

    public Set<String> membershipTypeNames() {
        return Collections.unmodifiableSet(membershipTypeNames);
    }

    // -- helpers ---------------------------------------------------------------

    private static String getStringFeature(FeatureStructure fs, String shortName) {
        Feature feature = fs.getType().getFeatureByBaseName(shortName);
        if (feature == null) {
            return null;
        }
        return fs.getStringValue(feature);
    }

    private static FeatureStructure getFeatureValue(FeatureStructure fs, String shortName) {
        Feature feature = fs.getType().getFeatureByBaseName(shortName);
        if (feature == null) {
            return null;
        }
        return fs.getFeatureValue(feature);
    }
}
