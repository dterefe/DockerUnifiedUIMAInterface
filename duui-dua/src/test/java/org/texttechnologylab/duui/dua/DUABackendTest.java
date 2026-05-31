package org.texttechnologylab.duui.dua;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.OptionalInt;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.dua.backend.DUAStoreRole;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpan;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpanQuery;
import org.texttechnologylab.duui.dua.query.DUATypeQuery;
import org.texttechnologylab.duui.dua.store.DUATypeNode;
import org.texttechnologylab.duui.dua.uima.storage.DUAConcurrentMemoryCasStorage;

class DUABackendTest {
    @Test
    void backendInstallsApacheAdapterWithoutLeakingStorageAsPublicRuntimeApi() throws Exception {
        DUABackend backend = DUABackend.over(new DUAConcurrentMemoryCasStorage());
        JCas view = JCasFactory.createJCas();

        backend.installInto(view);
        backend.writeIntSlot(7, 101, "begin", 42);

        assertSame(backend.apacheBackendAdapter(), view.getCasImpl().getBaseCAS().backend());
        assertEquals(OptionalInt.of(42), backend.readIntSlot(7, 101, "begin"));
        assertTrue(backend.layout().store(DUAStoreRole.RELATIONAL_VALUE).isPresent());
        assertTrue(backend.layout().store(DUAStoreRole.ANNOTATION_RANGE).isPresent());
        assertSame(backend.stores().annotationIndex(), backend.annotationIndex());
        assertSame(backend.stores().typesystemIndex(), backend.typesystemIndex());
        assertSame(backend.stores().values(), backend.values());
        assertSame(backend.stores().texts(), backend.texts());
    }

    @Test
    void duaDelegatesSemanticAnnotationQueriesToBackend() {
        DUAAnnotationSpan span = new DUAAnnotationSpan(
                100,
                11,
                3,
                5,
                12,
                Optional.of("example"));

        try (DUA dua = DUA.open(DUABackend.inMemory())) {
            dua.indexAnnotation(span);

            assertTrue(dua.findAnnotations(new DUAAnnotationSpanQuery.CoveringPoint(
                    100, 7, OptionalInt.of(3))).anyMatch(span::equals));
        }
    }

    @Test
    void duaDelegatesTypesystemQueriesToBackendIndex() {
        DUATypeNode top = new DUATypeNode(1, "uima.cas.TOP", OptionalInt.empty());
        DUATypeNode annotation = new DUATypeNode(2, "uima.tcas.Annotation", OptionalInt.of(1));
        DUATypeNode token = new DUATypeNode(3, "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token",
                OptionalInt.of(2));

        try (DUA dua = DUA.open(DUABackend.inMemory())) {
            dua.indexType(top);
            dua.indexType(annotation);
            dua.indexType(token);

            assertTrue(dua.findTypes(new DUATypeQuery.ExactType(token.typeName())).anyMatch(token::equals));
            assertTrue(dua.findTypes(new DUATypeQuery.Subtypes(top.typeName(), true)).anyMatch(token::equals));
            assertTrue(dua.findTypes(new DUATypeQuery.Supertypes(token.typeName(), true)).anyMatch(top::equals));
        }
    }
}
