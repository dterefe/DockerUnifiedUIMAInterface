package org.texttechnologylab.duui.dua.model;

import java.util.Objects;
import java.util.Optional;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAAddress(DUAId universeId, Optional<DUAId> corpusId, Optional<DUAId> documentId,
                         Optional<String> viewName, Optional<DUAId> featureStructureId) {
    public DUAAddress {
        Objects.requireNonNull(universeId, "universeId");
        corpusId = corpusId == null ? Optional.empty() : corpusId;
        documentId = documentId == null ? Optional.empty() : documentId;
        viewName = viewName == null ? Optional.empty() : viewName;
        featureStructureId = featureStructureId == null ? Optional.empty() : featureStructureId;
    }

    public static DUAAddress universe(DUAId universeId) {
        return new DUAAddress(universeId, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public DUAAddress corpus(DUAId corpusId) {
        return new DUAAddress(universeId, Optional.of(corpusId), documentId, viewName, featureStructureId);
    }

    public DUAAddress document(DUAId documentId) {
        return new DUAAddress(universeId, corpusId, Optional.of(documentId), viewName, featureStructureId);
    }

    public DUAAddress view(String viewName) {
        return new DUAAddress(universeId, corpusId, documentId, Optional.of(viewName), featureStructureId);
    }

    public DUAAddress featureStructure(DUAId featureStructureId) {
        return new DUAAddress(universeId, corpusId, documentId, viewName, Optional.of(featureStructureId));
    }
}
