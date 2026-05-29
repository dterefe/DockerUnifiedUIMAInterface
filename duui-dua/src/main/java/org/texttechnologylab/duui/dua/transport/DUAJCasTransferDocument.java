package org.texttechnologylab.duui.dua.transport;

import java.util.List;
import org.apache.uima.jcas.JCas;

public record DUAJCasTransferDocument(String documentId, List<String> corpusIds, JCas jCas) {
    public DUAJCasTransferDocument {
        if (documentId == null || documentId.isBlank()) {
            throw new IllegalArgumentException("documentId must not be blank");
        }
        corpusIds = corpusIds == null ? List.of() : List.copyOf(corpusIds);
        if (jCas == null) {
            throw new IllegalArgumentException("jCas must not be null");
        }
    }
}
