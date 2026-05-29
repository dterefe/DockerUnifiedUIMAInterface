package org.texttechnologylab.duui.dua.model;

import org.texttechnologylab.duui.dua.DUAId;

public sealed interface DUAEntity permits DUAUniverse, DUACorpus, DUADocument, DUAView, DUASofa,
        DUAType, DUAFeature, DUAFeatureStructure, DUAPayloadArtifact, DUAPipelineArtifact,
        DUADomainUnit, DUAAssociation {
    DUAId id();

    DUAEntityKind kind();
}
