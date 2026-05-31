package org.texttechnologylab.duui.dua;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.dua.projection.DUAProjection;
import org.texttechnologylab.duui.dua.projection.DUAProjectionType;
import org.texttechnologylab.duui.dua.projection.DUAProjectionTypes;

class DUAProjectionTest {
    @Test
    void backendRegistersCorpusAndDocumentProjectionMetadata() {
        try (DUABackend backend = DUABackend.inMemory()) {
            DUAProjection<DUAProjectionTypes.Corpus> corpus = backend.projections()
                    .find(DUAProjectionTypes.CORPUS)
                    .orElseThrow();
            DUAProjection<DUAProjectionTypes.Document> document = backend.projections()
                    .find(DUAProjectionTypes.Document.class)
                    .orElseThrow();

            assertEquals(DUAProjectionTypes.UCE_CORPUS_TYPE_NAME, corpus.typeName());
            assertEquals(DUAProjectionTypes.Corpus.class, corpus.markerClass());
            assertEquals("JDUA<Corpus>", corpus.modeName());
            assertEquals(DUAProjectionTypes.UCE_DOCUMENT_TYPE_NAME, document.typeName());
            assertEquals(DUAProjectionTypes.Document.class, document.markerClass());
            assertEquals("JDUA<Document>", document.modeName());
            assertTrue(backend.projections().stream().anyMatch(projection ->
                    DUAProjectionTypes.UCE_CORPUS_TYPE_NAME.equals(projection.typeName())));
        }
    }

    @Test
    void duaProjectsCorpusAndDocumentOverSameInstalledView() throws Exception {
        try (DUA dua = DUA.create()) {
            JCas view = dua.requireView();

            JDUA<DUAProjectionTypes.Corpus> corpus = dua.projectCorpus();
            JDUA<DUAProjectionTypes.Document> document = dua.projectDocument();

            assertSame(view, corpus.view());
            assertSame(view, document.view());
            assertSame(dua.backend(), corpus.backend());
            assertSame(dua.backend(), document.backend());
            assertSame(dua.backend().apacheBackendAdapter(), view.getCasImpl().getBaseCAS().backend());
            assertEquals("JDUA<Corpus>", corpus.modeName());
            assertEquals("JDUA<Document>", document.modeName());
        }
    }

    @Test
    void customProjectionMarkersSupportFutureDomainSpecificModes() throws Exception {
        DUAProjectionType<Journal> journalType =
                DUAProjectionTypes.type("org.example.annotation.Journal", Journal.class);

        try (DUA dua = DUA.create()) {
            DUAProjection<Journal> registered = dua.registerProjection(journalType);
            JDUA<Journal> journal = dua.project(journalType);

            assertSame(registered, journal.projection());
            assertEquals("org.example.annotation.Journal", journal.typeName());
            assertEquals(Journal.class, journal.markerClass());
            assertEquals("JDUA<Journal>", journal.modeName());
            assertSame(dua.requireView(), journal.view());
            assertSame(dua.backend().apacheBackendAdapter(), journal.view().getCasImpl().getBaseCAS().backend());
        }
    }

    private interface Journal {
    }
}
