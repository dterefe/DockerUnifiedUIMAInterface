package org.texttechnologylab.duui.dua;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.IntegerArray;
import org.apache.uima.jcas.cas.StringArray;
import org.apache.uima.jcas.tcas.Annotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.dua.uima.DUACasBackendInstaller;
import org.texttechnologylab.duui.dua.uima.storage.DUACasArrayKind;
import org.texttechnologylab.duui.dua.uima.storage.DUACasValue;
import org.texttechnologylab.duui.dua.uima.storage.DUAConcurrentMemoryCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUAOrderedKvCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUASqliteCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUAStorageBackend;

class DUACasStorageBackendTest {
    @TempDir
    Path temp;

    @Test
    void concurrentMemoryBackendMatchesHeapCasForBasicReads() throws Exception {
        assertEquals(exercise(JCasFactory.createJCas()), exercise(withConcurrentMemoryBackend()));
    }

    @Test
    void sqliteBackendMatchesHeapCasForBasicReads() throws Exception {
        Path sqlite = Files.createTempFile("dua-cas-storage", ".sqlite");
        try (DUASqliteCasStorage storage = new DUASqliteCasStorage(sqlite)) {
            JCas view = JCasFactory.createJCas();
            DUACasBackendInstaller.install(view, new DUAStorageBackend(storage));
            assertEquals(exercise(JCasFactory.createJCas()), exercise(view));
        }
    }

    @Test
    void sqliteStorageInitializesFromEmbeddedSchemaOnCleanResourcePath() {
        Path sqlite = temp.resolve("embedded-schema.sqlite");
        try (DUASqliteCasStorage storage = new DUASqliteCasStorage(sqlite)) {
            storage.writeIntSlot(1, 101, "begin", 17);
            storage.initializeArray(DUACasArrayKind.STRING, 2, 1);
            storage.writeArrayValue(DUACasArrayKind.STRING, 2, 0, DUACasValue.of("clean"));
        }

        try (DUASqliteCasStorage storage = new DUASqliteCasStorage(sqlite)) {
            assertEquals(17, storage.readIntSlotOrDefault(1, 101, "begin", 0));
            assertEquals("clean", storage.readArrayValue(DUACasArrayKind.STRING, 2, 0)
                    .orElseThrow()
                    .stringValue());
        }
    }

    @Test
    void orderedKvBackendMatchesHeapCasForBasicReads() throws Exception {
        try (DUAOrderedKvCasStorage storage = new DUAOrderedKvCasStorage(temp.resolve("ordered-kv"))) {
            JCas view = JCasFactory.createJCas();
            DUACasBackendInstaller.install(view, new DUAStorageBackend(storage));
            assertEquals(exercise(JCasFactory.createJCas()), exercise(view));
        }
    }

    @Test
    void orderedKvBackendReopensTypedSlotsAndArraysFromWal() {
        Path directory = temp.resolve("ordered-kv-reopen");
        try (DUAOrderedKvCasStorage storage = new DUAOrderedKvCasStorage(directory)) {
            storage.writeIntSlot(1, 101, "begin", 42);
            storage.writeSlot(1, 102, "name", DUACasValue.of("dua"));
            storage.writeSlot(1, 103, "target", DUACasValue.ref(99));
            storage.initializeArray(DUACasArrayKind.INTEGER, 2, 3);
            storage.writeArrayValue(DUACasArrayKind.INTEGER, 2, 1, DUACasValue.ofInt(7));
            storage.initializeArray(DUACasArrayKind.STRING, 3, 2);
            storage.writeArrayValue(DUACasArrayKind.STRING, 3, 0, DUACasValue.of("token"));
        }

        try (DUAOrderedKvCasStorage storage = new DUAOrderedKvCasStorage(directory)) {
            assertEquals(42, storage.readIntSlotOrDefault(1, 101, "begin", 0));
            assertEquals("dua", storage.readSlot(1, 102, "name").orElseThrow().stringValue());
            assertEquals(99, storage.readSlot(1, 103, "target").orElseThrow().intValue());
            assertEquals(3, storage.arraySize(DUACasArrayKind.INTEGER, 2));
            assertEquals(7, storage.readArrayValue(DUACasArrayKind.INTEGER, 2, 1).orElseThrow().intValue());
            assertEquals(2, storage.arraySize(DUACasArrayKind.STRING, 3));
            assertEquals("token", storage.readArrayValue(DUACasArrayKind.STRING, 3, 0).orElseThrow().stringValue());
        }
    }

    @Test
    void concurrentMemoryStorageSupportsVirtualThreadWritesToIndependentSlots() throws Exception {
        JCas view = withConcurrentMemoryBackend();
        DUAStorageBackend backend = (DUAStorageBackend) view.getCasImpl().getBaseCAS().backend();
        Type annotationType = view.getTypeSystem().getType(CAS.TYPE_NAME_ANNOTATION);
        Feature begin = annotationType.getFeatureByBaseName("begin");

        int count = 1_000;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                final int fsRef = i + 1;
                final int value = i * 2;
                futures.add(executor.submit(() -> backend.slots().setIntValue(fsRef, begin, value)));
            }
            for (var future : futures) {
                future.get();
            }
        }

        for (int i = 0; i < count; i++) {
            assertEquals(i * 2, backend.slots().getIntValue(i + 1, begin));
        }
    }

    private static JCas withConcurrentMemoryBackend() throws Exception {
        JCas view = JCasFactory.createJCas();
        DUACasBackendInstaller.install(view, new DUAStorageBackend(new DUAConcurrentMemoryCasStorage()));
        return view;
    }

    private static Snapshot exercise(JCas view) {
        view.setDocumentText("abcdef");
        Annotation annotation = new Annotation(view, 1, 4);
        annotation.setBegin(2);
        annotation.setEnd(5);

        IntegerArray integers = new IntegerArray(view, 5);
        integers.set(3, 42);

        StringArray strings = new StringArray(view, 3);
        strings.set(1, "dua");

        return new Snapshot(
                annotation.getBegin(),
                annotation.getEnd(),
                integers.size(),
                integers.get(0),
                integers.get(3),
                strings.size(),
                strings.get(0),
                strings.get(1));
    }

    private record Snapshot(int begin, int end, int intSize, int intDefault, int intValue,
                            int stringSize, String stringDefault, String stringValue) {
    }
}
