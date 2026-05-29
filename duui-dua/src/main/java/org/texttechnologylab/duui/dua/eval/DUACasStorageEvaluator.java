package org.texttechnologylab.duui.dua.eval;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.texttechnologylab.duui.dua.uima.DUACasBackendInstaller;
import org.texttechnologylab.duui.dua.uima.storage.DUAConcurrentMemoryCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUACachedCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUADenseMemoryCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUAOrderedKvCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUASqliteCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUAStorageBackend;
import org.texttechnologylab.duui.dua.uima.storage.DUATieredCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUATieredWritePolicy;

public final class DUACasStorageEvaluator {
    private DUACasStorageEvaluator() {
    }

    public static DUAStorageEvaluationReport evaluateSmoke() throws Exception {
        return evaluate(DUAEvaluationWorkload.smoke(), true);
    }

    public static DUAStorageEvaluationReport evaluate(DUAEvaluationWorkload workload, boolean includeSqlite) throws Exception {
        List<DUAEvaluationResult> results = new ArrayList<>();
        results.add(evaluateJCas("uima-heap-baseline", workload, JCasFactory.createJCas()));
        JCas concurrentMemory = JCasFactory.createJCas();
        DUACasBackendInstaller.install(concurrentMemory, new DUAStorageBackend(new DUAConcurrentMemoryCasStorage()));
        results.add(evaluateJCas("dua-concurrent-memory", workload, concurrentMemory));
        JCas denseMemory = JCasFactory.createJCas();
        DUACasBackendInstaller.install(denseMemory, new DUAStorageBackend(new DUADenseMemoryCasStorage()));
        results.add(evaluateJCas("dua-dense-memory", workload, denseMemory));
        if (includeSqlite) {
            var orderedKvPath = Files.createTempDirectory("dua-eval-ordered-kv");
            try (DUAOrderedKvCasStorage storage = new DUAOrderedKvCasStorage(orderedKvPath)) {
                JCas orderedKv = JCasFactory.createJCas();
                DUACasBackendInstaller.install(orderedKv, new DUAStorageBackend(storage));
                results.add(evaluateJCas("dua-ordered-kv-wal", workload, orderedKv));
            }
            var tieredKvPath = Files.createTempDirectory("dua-eval-tiered-kv");
            try (DUAOrderedKvCasStorage storage = new DUAOrderedKvCasStorage(tieredKvPath);
                 DUATieredCasStorage tiered = new DUATieredCasStorage(
                         storage, workload.featureStructures() + 1_024, DUATieredWritePolicy.WRITE_BACK)) {
                JCas orderedKv = JCasFactory.createJCas();
                DUACasBackendInstaller.install(orderedKv, new DUAStorageBackend(tiered));
                results.add(evaluateJCas("dua-tiered-ordered-kv-writeback", workload, orderedKv));
            }
            var sqlitePath = Files.createTempFile("dua-eval", ".sqlite");
            try (DUASqliteCasStorage storage = new DUASqliteCasStorage(sqlitePath)) {
                JCas sqlite = JCasFactory.createJCas();
                DUACasBackendInstaller.install(sqlite, new DUAStorageBackend(storage));
                results.add(evaluateJCas("dua-sqlite-typed", workload, sqlite));
            }
            var cachedSqlitePath = Files.createTempFile("dua-eval-cached", ".sqlite");
            try (DUASqliteCasStorage storage = new DUASqliteCasStorage(cachedSqlitePath)) {
                JCas sqlite = JCasFactory.createJCas();
                DUACasBackendInstaller.install(sqlite,
                        new DUAStorageBackend(new DUACachedCasStorage(storage, workload.featureStructures() * 4)));
                results.add(evaluateJCas("dua-sqlite-typed-bounded-cache", workload, sqlite));
            }
            var tieredSqlitePath = Files.createTempFile("dua-eval-tiered", ".sqlite");
            try (DUASqliteCasStorage storage = new DUASqliteCasStorage(tieredSqlitePath);
                 DUATieredCasStorage tiered = new DUATieredCasStorage(
                         storage, workload.featureStructures() + 1_024, DUATieredWritePolicy.WRITE_BACK)) {
                JCas sqlite = JCasFactory.createJCas();
                DUACasBackendInstaller.install(sqlite, new DUAStorageBackend(tiered));
                results.add(evaluateJCas("dua-tiered-sqlite-writeback", workload, sqlite));
            }
        }
        return new DUAStorageEvaluationReport(results);
    }

    private static DUAEvaluationResult evaluateJCas(String name, DUAEvaluationWorkload workload, JCas jCas) throws Exception {
        forceGc();
        long beforeMemory = usedMemory();
        List<Annotation> annotations = new ArrayList<>(workload.featureStructures());

        long writeStart = System.nanoTime();
        for (int i = 0; i < workload.featureStructures(); i++) {
            Annotation annotation = new Annotation(jCas);
            annotation.setBegin(i);
            annotation.setEnd(i + 1);
            annotations.add(annotation);
        }
        long writeNanos = System.nanoTime() - writeStart;

        long checksum = 0;
        long readStart = System.nanoTime();
        for (int iteration = 0; iteration < workload.readIterations(); iteration++) {
            for (Annotation annotation : annotations) {
                checksum += annotation.getBegin();
                checksum += annotation.getEnd();
            }
        }
        long readNanos = System.nanoTime() - readStart;

        long concurrentWriteNanos = evaluateConcurrentSlotWrites(jCas, workload);
        forceGc();
        long memoryDelta = usedMemory() - beforeMemory;

        return new DUAEvaluationResult(name, workload, writeNanos, readNanos, concurrentWriteNanos, memoryDelta, checksum);
    }

    private static long evaluateConcurrentSlotWrites(JCas jCas, DUAEvaluationWorkload workload) throws Exception {
        Type annotationType = jCas.getTypeSystem().getType(CAS.TYPE_NAME_ANNOTATION);
        Feature begin = annotationType.getFeatureByBaseName("begin");
        DUAStorageBackend backend = jCas.getCasImpl().getBaseCAS().backend() instanceof DUAStorageBackend storageBackend
                ? storageBackend
                : null;

        long start = System.nanoTime();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>(workload.virtualThreads());
            for (int thread = 0; thread < workload.virtualThreads(); thread++) {
                final int offset = thread * workload.writesPerThread();
                futures.add(executor.submit(() -> {
                    if (backend != null) {
                        for (int i = 0; i < workload.writesPerThread(); i++) {
                            backend.slots().setIntValue(offset + i + 1, begin, i);
                        }
                    } else {
                        Annotation annotation = new Annotation(jCas);
                        for (int i = 0; i < workload.writesPerThread(); i++) {
                            annotation.setBegin(i);
                        }
                    }
                }));
            }
            for (var future : futures) {
                future.get();
            }
        }
        return System.nanoTime() - start;
    }

    private static long usedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void forceGc() throws InterruptedException {
        System.gc();
        Thread.sleep(25);
    }
}
