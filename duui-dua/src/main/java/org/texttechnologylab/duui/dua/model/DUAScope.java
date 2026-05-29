package org.texttechnologylab.duui.dua.model;

import org.texttechnologylab.duui.dua.DUAId;

public sealed interface DUAScope permits DUAScope.UniverseScope, DUAScope.CorpusScope,
        DUAScope.DocumentScope, DUAScope.ViewScope, DUAScope.FeatureStructureScope {
    DUAAddress address();

    record UniverseScope(DUAAddress address) implements DUAScope {
        public UniverseScope(DUAId universeId) {
            this(DUAAddress.universe(universeId));
        }
    }

    record CorpusScope(DUAAddress address) implements DUAScope {
        public CorpusScope(DUAId universeId, DUAId corpusId) {
            this(DUAAddress.universe(universeId).corpus(corpusId));
        }
    }

    record DocumentScope(DUAAddress address) implements DUAScope {
        public DocumentScope(DUAId universeId, DUAId corpusId, DUAId documentId) {
            this(DUAAddress.universe(universeId).corpus(corpusId).document(documentId));
        }
    }

    record ViewScope(DUAAddress address) implements DUAScope {
        public ViewScope(DUAId universeId, DUAId corpusId, DUAId documentId, String viewName) {
            this(DUAAddress.universe(universeId).corpus(corpusId).document(documentId).view(viewName));
        }
    }

    record FeatureStructureScope(DUAAddress address) implements DUAScope {
        public FeatureStructureScope(DUAAddress address, DUAId featureStructureId) {
            this(address.featureStructure(featureStructureId));
        }
    }
}
