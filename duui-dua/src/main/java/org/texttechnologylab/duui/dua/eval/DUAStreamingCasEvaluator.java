package org.texttechnologylab.duui.dua.eval;

import java.nio.file.Files;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.texttechnologylab.duui.dua.uima.DUACasBackendInstaller;
import org.texttechnologylab.duui.dua.uima.storage.DUADenseMemoryCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUASqliteCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUAStorageBackend;

public final class DUAStreamingCasEvaluator {
    private DUAStreamingCasEvaluator() {
    }

    public static DUAStreamingEvaluationResult evaluateDenseEphemeral(int documents, int annotationsPerDocument)
            throws Exception {
        return evaluate("dua-dense-ephemeral-stream", documents, annotationsPerDocument, documentIndex -> {
            JCas jCas = JCasFactory.createJCas();
            DUACasBackendInstaller.install(jCas, new DUAStorageBackend(new DUADenseMemoryCasStorage()));
            return jCas;
        });
    }

    public static DUAStreamingEvaluationResult evaluateSqlitePersistent(int documents, int annotationsPerDocument)
            throws Exception {
        var sqlitePath = Files.createTempFile("dua-stream", ".sqlite");
        try (DUASqliteCasStorage storage = new DUASqliteCasStorage(sqlitePath)) {
            return evaluate("dua-sqlite-persistent-stream", documents, annotationsPerDocument, documentIndex -> {
                JCas jCas = JCasFactory.createJCas();
                DUACasBackendInstaller.install(jCas, new DUAStorageBackend(storage));
                return jCas;
            });
        }
    }

    private static DUAStreamingEvaluationResult evaluate(String name,
                                                        int documents,
                                                        int annotationsPerDocument,
                                                        JCasFactoryForDocument factory) throws Exception {
        if (documents < 1 || annotationsPerDocument < 1) {
            throw new IllegalArgumentException("documents and annotationsPerDocument must be positive");
        }
        forceGc();
        long baseline = usedMemory();
        long maxLiveDelta = 0;
        long checksum = 0;
        for (int documentIndex = 0; documentIndex < documents; documentIndex++) {
            JCas jCas = factory.create(documentIndex);
            jCas.setDocumentText("document-" + documentIndex);
            for (int i = 0; i < annotationsPerDocument; i++) {
                Annotation annotation = new Annotation(jCas, i, i + 1);
                checksum += annotation.getBegin();
                checksum += annotation.getEnd();
            }
            jCas = null;
            forceGc();
            maxLiveDelta = Math.max(maxLiveDelta, usedMemory() - baseline);
        }
        forceGc();
        long finalDelta = usedMemory() - baseline;
        return new DUAStreamingEvaluationResult(name, documents, annotationsPerDocument,
                maxLiveDelta, finalDelta, checksum);
    }

    private static long usedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void forceGc() throws InterruptedException {
        System.gc();
        Thread.sleep(25);
    }

    @FunctionalInterface
    private interface JCasFactoryForDocument {
        JCas create(int documentIndex) throws Exception;
    }
}
