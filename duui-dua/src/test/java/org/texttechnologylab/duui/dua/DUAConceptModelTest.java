package org.texttechnologylab.duui.dua;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.dua.inspect.DUADataBinding;
import org.texttechnologylab.duui.dua.inspect.DUATimelineBinding;
import org.texttechnologylab.duui.dua.model.DUAAssociationType;
import org.texttechnologylab.duui.dua.model.DUADomainUnit;
import org.texttechnologylab.duui.dua.model.DUAEntityKind;
import org.texttechnologylab.duui.dua.model.DUAEntityRef;
import org.texttechnologylab.duui.dua.model.DUAEquivalenceAssociation;
import org.texttechnologylab.duui.dua.model.DUAFeatureKey;
import org.texttechnologylab.duui.dua.model.DUAScope;
import org.texttechnologylab.duui.dua.query.DUAQuery;

class DUAConceptModelTest {
    @Test
    void capturesDomainAssociationsAndInspectorBindings() {
        DUAId universeId = DUAId.create();
        DUAId corpusId = DUAId.create();
        DUAScope scope = new DUAScope.CorpusScope(universeId, corpusId);

        DUADomainUnit firstArticle = new DUADomainUnit(
                DUAId.create(),
                "news article in xmi a",
                Optional.empty(),
                scope,
                new DUAEntityRef<>(DUAId.create(), DUAEntityKind.PAYLOAD_ARTIFACT),
                Map.of());
        DUADomainUnit secondArticle = new DUADomainUnit(
                DUAId.create(),
                "news article in xmi b",
                Optional.empty(),
                scope,
                new DUAEntityRef<>(DUAId.create(), DUAEntityKind.PAYLOAD_ARTIFACT),
                Map.of());

        DUAEquivalenceAssociation equivalence = new DUAEquivalenceAssociation(
                DUAId.create(),
                "same-news-article",
                DUAEntityRef.of(firstArticle),
                DUAEntityRef.of(secondArticle),
                "canonical article identity",
                Map.of());

        DUAFeatureKey start = new DUAFeatureKey("org.example.Event", "startDate");
        DUAFeatureKey label = new DUAFeatureKey("org.example.Event", "label");
        DUATimelineBinding timeline = new DUATimelineBinding(start, Optional.empty(), label, Optional.empty());
        DUADataBinding binding = new DUADataBinding(new DUAQuery.MatchAll(), Map.of("label", timeline.label()));

        assertEquals(DUAAssociationType.EQUIVALENCE, DUAAssociationType.of(equivalence));
        assertEquals("org.example.Event#label", binding.featureMappings().get("label").qualifiedName());
    }
}
