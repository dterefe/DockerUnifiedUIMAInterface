package org.texttechnologylab.duui.dua.eval;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import org.texttechnologylab.duui.dua.uima.storage.DUACasArrayKind;
import org.texttechnologylab.duui.dua.uima.storage.DUACasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUACasValue;
import org.texttechnologylab.duui.dua.uima.storage.DUAConcurrentMemoryCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUADenseMemoryCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUAOrderedKvCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUASqliteCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUATieredCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUATieredWritePolicy;

public final class DUAUceBackendStressEvaluator {
    private static final int TYPE_DOCUMENT = 1;
    private static final int TYPE_PAGE = 2;
    private static final int TYPE_ANNOTATION = 3;
    private static final int TYPE_METADATA = 4;
    private static final int TYPE_SRL_EVENT = 5;
    private static final int TYPE_ASSOCIATION = 6;
    private static final int VIEW_DEFAULT = 1;

    private static final int F_DOCUMENT_ID = 101;
    private static final int F_TITLE_TERM = 102;
    private static final int F_PUBLISHED_YEAR = 103;
    private static final int F_PAGE_TERM = 201;
    private static final int F_PAGE_DOCUMENT = 202;
    private static final int F_BEGIN = 301;
    private static final int F_END = 302;
    private static final int F_ANNOTATION_TYPE = 303;
    private static final int F_COVERED_TERM = 304;
    private static final int F_METADATA_KEY = 401;
    private static final int F_METADATA_TERM = 402;
    private static final int F_METADATA_NUMBER = 403;
    private static final int F_SRL_DOCUMENT = 501;
    private static final int F_SRL_VERB_TERM = 502;
    private static final int F_SRL_ROLE_MASK = 503;
    private static final int F_SRL_ARG0_TERM = 504;
    private static final int F_SRL_ARG1_TERM = 505;
    private static final int F_EDGE_SOURCE = 601;
    private static final int F_EDGE_TARGET = 602;
    private static final int F_EDGE_KIND = 603;
    private static final int F_EDGE_SEQUENCE = 604;
    private static final int F_CONCURRENT_VALUE = 701;

    private static final String N_DOCUMENT_ID = "dua:documentId";
    private static final String N_TITLE_TERM = "dua:titleTerm";
    private static final String N_PUBLISHED_YEAR = "dua:publishedYear";
    private static final String N_PAGE_TERM = "dua:pageTerm";
    private static final String N_PAGE_DOCUMENT = "dua:pageDocument";
    private static final String N_BEGIN = "uima:begin";
    private static final String N_END = "uima:end";
    private static final String N_ANNOTATION_TYPE = "dua:annotationType";
    private static final String N_COVERED_TERM = "dua:coveredTerm";
    private static final String N_METADATA_KEY = "dua:metadataKey";
    private static final String N_METADATA_TERM = "dua:metadataTerm";
    private static final String N_METADATA_NUMBER = "dua:metadataNumber";
    private static final String N_SRL_DOCUMENT = "dua:srlDocument";
    private static final String N_SRL_VERB_TERM = "dua:srlVerbTerm";
    private static final String N_SRL_ROLE_MASK = "dua:srlRoleMask";
    private static final String N_SRL_ARG0_TERM = "dua:srlArg0Term";
    private static final String N_SRL_ARG1_TERM = "dua:srlArg1Term";
    private static final String N_EDGE_SOURCE = "dua:edgeSource";
    private static final String N_EDGE_TARGET = "dua:edgeTarget";
    private static final String N_EDGE_KIND = "dua:edgeKind";
    private static final String N_EDGE_SEQUENCE = "dua:edgeSequence";
    private static final String N_CONCURRENT_VALUE = "dua:concurrentValue";

    private DUAUceBackendStressEvaluator() {
    }

    public static DUAUceStressReport evaluateSmoke() throws Exception {
        return evaluate(DUAUceStressProfile.smoke(), true);
    }

    public static DUAUceStressReport evaluate(DUAUceStressProfile profile, boolean includeSqlite) throws Exception {
        List<DUAUceStressResult> results = new ArrayList<>();
        try (DUACasStorage storage = new DUAConcurrentMemoryCasStorage()) {
            results.add(evaluateStorage("dua-concurrent-memory", profile, storage));
        }
        try (DUACasStorage storage = new DUADenseMemoryCasStorage()) {
            results.add(evaluateStorage("dua-dense-memory", profile, storage));
        }
        if (includeSqlite) {
            var orderedKvPath = Files.createTempDirectory("dua-uce-ordered-kv");
            try (DUACasStorage storage = new DUAOrderedKvCasStorage(orderedKvPath)) {
                results.add(evaluateStorage("dua-ordered-kv-wal", profile, storage));
            }
            var tieredKvPath = Files.createTempDirectory("dua-uce-tiered-kv");
            try (DUAOrderedKvCasStorage durable = new DUAOrderedKvCasStorage(tieredKvPath);
                 DUATieredCasStorage storage = new DUATieredCasStorage(
                         durable,
                         tieredHotSetCapacity(profile),
                         DUATieredWritePolicy.WRITE_BACK)) {
                results.add(evaluateStorage("dua-tiered-ordered-kv-writeback", profile, storage));
            }
            var sqlitePath = Files.createTempFile("dua-uce-stress", ".sqlite");
            try (DUACasStorage storage = new DUASqliteCasStorage(sqlitePath)) {
                results.add(evaluateStorage("dua-sqlite-typed", profile, storage));
            }
            var tieredPath = Files.createTempFile("dua-uce-tiered-stress", ".sqlite");
            try (DUASqliteCasStorage durable = new DUASqliteCasStorage(tieredPath);
                 DUATieredCasStorage storage = new DUATieredCasStorage(
                         durable,
                         tieredHotSetCapacity(profile),
                         DUATieredWritePolicy.WRITE_BACK)) {
                results.add(evaluateStorage("dua-tiered-sqlite-writeback", profile, storage));
            }
        }
        return new DUAUceStressReport(results);
    }

    private static DUAUceStressResult evaluateStorage(String backendName,
                                                     DUAUceStressProfile profile,
                                                     DUACasStorage storage) throws Exception {
        forceGc();
        long beforeMemory = usedMemory();
        SyntheticCorpus corpus = new SyntheticCorpus(profile);

        long ingestStart = System.nanoTime();
        ingest(storage, corpus);
        long ingestNanos = System.nanoTime() - ingestStart;

        long checksum = 0;

        long fulltextStart = System.nanoTime();
        checksum += stressFulltextLikeSearch(storage, corpus);
        long fulltextNanos = System.nanoTime() - fulltextStart;

        long metadataStart = System.nanoTime();
        checksum += stressMetadataFilters(storage, corpus);
        long metadataNanos = System.nanoTime() - metadataStart;

        long srlStart = System.nanoTime();
        checksum += stressSemanticRoleSearch(storage, corpus);
        long semanticRoleNanos = System.nanoTime() - srlStart;

        long annotationStart = System.nanoTime();
        checksum += stressAnnotationSummaries(storage, corpus);
        long annotationSummaryNanos = System.nanoTime() - annotationStart;

        long associationStart = System.nanoTime();
        checksum += stressAssociationNeighborhoods(storage, corpus);
        long associationNanos = System.nanoTime() - associationStart;

        long concurrentWriteNanos = stressConcurrentWrites(storage, corpus);
        if (storage instanceof DUATieredCasStorage tiered) {
            tiered.flush();
        }

        forceGc();
        long memoryDelta = usedMemory() - beforeMemory;
        return new DUAUceStressResult(backendName, profile, ingestNanos, fulltextNanos,
                metadataNanos, semanticRoleNanos, annotationSummaryNanos, associationNanos,
                concurrentWriteNanos, memoryDelta, checksum);
    }

    private static void ingest(DUACasStorage storage, SyntheticCorpus corpus) {
        DUAUceStressProfile profile = corpus.profile;
        int pageIndex = 0;
        int annotationIndex = 0;
        int metadataIndex = 0;
        int srlIndex = 0;
        int associationIndex = 0;
        for (int documentIndex = 0; documentIndex < profile.documents(); documentIndex++) {
            int documentRef = storage.allocateFsId(TYPE_DOCUMENT, VIEW_DEFAULT);
            corpus.documents[documentIndex] = documentRef;
            writeInt(storage, documentRef, F_DOCUMENT_ID, N_DOCUMENT_ID, documentIndex + 1);
            writeInt(storage, documentRef, F_TITLE_TERM, N_TITLE_TERM, 10_000 + (documentIndex % 512));
            writeInt(storage, documentRef, F_PUBLISHED_YEAR, N_PUBLISHED_YEAR, 1800 + (documentIndex % 225));
            storage.initializeArray(DUACasArrayKind.FS, documentRef, profile.pagesPerDocument());

            for (int page = 0; page < profile.pagesPerDocument(); page++) {
                int pageRef = storage.allocateFsId(TYPE_PAGE, VIEW_DEFAULT);
                corpus.pages[pageIndex++] = pageRef;
                writeRef(storage, pageRef, F_PAGE_DOCUMENT, N_PAGE_DOCUMENT, documentRef);
                writeInt(storage, pageRef, F_PAGE_TERM, N_PAGE_TERM, 20_000 + ((documentIndex * 31 + page) % 128));
                writeInt(storage, pageRef, F_BEGIN, N_BEGIN, page * 1_000);
                writeInt(storage, pageRef, F_END, N_END, page * 1_000 + 999);
                storage.writeArrayValue(DUACasArrayKind.FS, documentRef, page, DUACasValue.ref(pageRef));
            }

            for (int metadata = 0; metadata < profile.metadataFieldsPerDocument(); metadata++) {
                int metadataRef = storage.allocateFsId(TYPE_METADATA, VIEW_DEFAULT);
                corpus.metadata[metadataIndex++] = metadataRef;
                writeRef(storage, metadataRef, F_PAGE_DOCUMENT, N_PAGE_DOCUMENT, documentRef);
                writeInt(storage, metadataRef, F_METADATA_KEY, N_METADATA_KEY, metadata);
                writeInt(storage, metadataRef, F_METADATA_TERM, N_METADATA_TERM, 30_000 + ((documentIndex + metadata) % 256));
                writeInt(storage, metadataRef, F_METADATA_NUMBER, N_METADATA_NUMBER, documentIndex * 10 + metadata);
            }

            int firstAnnotationIndex = annotationIndex;
            for (int annotation = 0; annotation < profile.annotationsPerDocument(); annotation++) {
                int annotationRef = storage.allocateFsId(TYPE_ANNOTATION, VIEW_DEFAULT);
                corpus.annotations[annotationIndex++] = annotationRef;
                writeRef(storage, annotationRef, F_PAGE_DOCUMENT, N_PAGE_DOCUMENT, documentRef);
                writeInt(storage, annotationRef, F_BEGIN, N_BEGIN, annotation * 7);
                writeInt(storage, annotationRef, F_END, N_END, annotation * 7 + 5);
                writeInt(storage, annotationRef, F_ANNOTATION_TYPE, N_ANNOTATION_TYPE, annotation % 12);
                writeInt(storage, annotationRef, F_COVERED_TERM, N_COVERED_TERM, 40_000 + ((documentIndex + annotation) % 512));
            }

            for (int event = 0; event < profile.semanticRoleEventsPerDocument(); event++) {
                int srlRef = storage.allocateFsId(TYPE_SRL_EVENT, VIEW_DEFAULT);
                corpus.semanticRoleEvents[srlIndex++] = srlRef;
                writeRef(storage, srlRef, F_SRL_DOCUMENT, N_SRL_DOCUMENT, documentRef);
                writeInt(storage, srlRef, F_BEGIN, N_BEGIN, event * 13);
                writeInt(storage, srlRef, F_SRL_VERB_TERM, N_SRL_VERB_TERM, 50_000 + (event % 64));
                writeInt(storage, srlRef, F_SRL_ROLE_MASK, N_SRL_ROLE_MASK, (1 << (event % 4)) | 1);
                writeInt(storage, srlRef, F_SRL_ARG0_TERM, N_SRL_ARG0_TERM, 60_000 + ((documentIndex + event) % 128));
                writeInt(storage, srlRef, F_SRL_ARG1_TERM, N_SRL_ARG1_TERM, 61_000 + ((documentIndex * 3 + event) % 128));
            }

            int annotationCount = profile.annotationsPerDocument();
            for (int edge = 0; edge < profile.associationsPerDocument(); edge++) {
                int edgeRef = storage.allocateFsId(TYPE_ASSOCIATION, VIEW_DEFAULT);
                corpus.associations[associationIndex++] = edgeRef;
                int source = corpus.annotations[firstAnnotationIndex + (edge % annotationCount)];
                int target = corpus.annotations[firstAnnotationIndex + ((edge * 7 + 1) % annotationCount)];
                writeRef(storage, edgeRef, F_EDGE_SOURCE, N_EDGE_SOURCE, source);
                writeRef(storage, edgeRef, F_EDGE_TARGET, N_EDGE_TARGET, target);
                writeInt(storage, edgeRef, F_EDGE_KIND, N_EDGE_KIND, 1 + (edge % 4));
                writeInt(storage, edgeRef, F_EDGE_SEQUENCE, N_EDGE_SEQUENCE, edge);
            }
        }
    }

    private static long stressFulltextLikeSearch(DUACasStorage storage, SyntheticCorpus corpus) {
        long checksum = 0;
        for (int iteration = 0; iteration < corpus.profile.queryIterations(); iteration++) {
            int targetTerm = 20_000 + (iteration % 128);
            for (int pageRef : corpus.pages) {
                int term = readInt(storage, pageRef, F_PAGE_TERM, N_PAGE_TERM);
                if (term == targetTerm) {
                    checksum += readInt(storage, pageRef, F_BEGIN, N_BEGIN);
                    checksum += readInt(storage, pageRef, F_END, N_END);
                    checksum += readInt(storage, pageRef, F_PAGE_DOCUMENT, N_PAGE_DOCUMENT);
                }
            }
        }
        return checksum;
    }

    private static long stressMetadataFilters(DUACasStorage storage, SyntheticCorpus corpus) {
        long checksum = 0;
        for (int iteration = 0; iteration < corpus.profile.queryIterations(); iteration++) {
            int key = iteration % corpus.profile.metadataFieldsPerDocument();
            int targetModulo = iteration % 256;
            for (int metadataRef : corpus.metadata) {
                if (readInt(storage, metadataRef, F_METADATA_KEY, N_METADATA_KEY) == key) {
                    int term = readInt(storage, metadataRef, F_METADATA_TERM, N_METADATA_TERM);
                    int number = readInt(storage, metadataRef, F_METADATA_NUMBER, N_METADATA_NUMBER);
                    if ((term - 30_000) % 256 == targetModulo || number % 97 == targetModulo % 97) {
                        checksum += term;
                        checksum += number;
                    }
                }
            }
        }
        return checksum;
    }

    private static long stressSemanticRoleSearch(DUACasStorage storage, SyntheticCorpus corpus) {
        long checksum = 0;
        for (int iteration = 0; iteration < corpus.profile.queryIterations(); iteration++) {
            int verb = 50_000 + (iteration % 64);
            int arg0Modulo = iteration % 128;
            for (int eventRef : corpus.semanticRoleEvents) {
                if (readInt(storage, eventRef, F_SRL_VERB_TERM, N_SRL_VERB_TERM) == verb
                        && (readInt(storage, eventRef, F_SRL_ROLE_MASK, N_SRL_ROLE_MASK) & 1) != 0) {
                    int arg0 = readInt(storage, eventRef, F_SRL_ARG0_TERM, N_SRL_ARG0_TERM);
                    if ((arg0 - 60_000) % 128 == arg0Modulo) {
                        checksum += readInt(storage, eventRef, F_BEGIN, N_BEGIN);
                        checksum += readInt(storage, eventRef, F_SRL_DOCUMENT, N_SRL_DOCUMENT);
                    }
                }
            }
        }
        return checksum;
    }

    private static long stressAnnotationSummaries(DUACasStorage storage, SyntheticCorpus corpus) {
        long checksum = 0;
        int[] buckets = new int[12 * 512];
        for (int iteration = 0; iteration < corpus.profile.queryIterations(); iteration++) {
            java.util.Arrays.fill(buckets, 0);
            for (int annotationRef : corpus.annotations) {
                int type = readInt(storage, annotationRef, F_ANNOTATION_TYPE, N_ANNOTATION_TYPE);
                int covered = readInt(storage, annotationRef, F_COVERED_TERM, N_COVERED_TERM) - 40_000;
                buckets[type * 512 + covered]++;
            }
            for (int count : buckets) {
                checksum += count;
            }
        }
        return checksum;
    }

    private static long stressAssociationNeighborhoods(DUACasStorage storage, SyntheticCorpus corpus) {
        long checksum = 0;
        for (int iteration = 0; iteration < corpus.profile.queryIterations(); iteration++) {
            int step = Math.max(1, corpus.associations.length / 512);
            for (int i = iteration % step; i < corpus.associations.length; i += step) {
                int edgeRef = corpus.associations[i];
                checksum += readInt(storage, edgeRef, F_EDGE_SOURCE, N_EDGE_SOURCE);
                checksum += readInt(storage, edgeRef, F_EDGE_TARGET, N_EDGE_TARGET);
                checksum += readInt(storage, edgeRef, F_EDGE_KIND, N_EDGE_KIND);
                checksum += readInt(storage, edgeRef, F_EDGE_SEQUENCE, N_EDGE_SEQUENCE);
            }
        }
        return checksum;
    }

    private static long stressConcurrentWrites(DUACasStorage storage, SyntheticCorpus corpus) throws Exception {
        long start = System.nanoTime();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>(corpus.profile.virtualThreads());
            for (int thread = 0; thread < corpus.profile.virtualThreads(); thread++) {
                final int threadIndex = thread;
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < corpus.profile.writesPerThread(); i++) {
                        int annotationRef = corpus.annotations[
                                Math.floorMod(threadIndex * corpus.profile.writesPerThread() + i,
                                        corpus.annotations.length)];
                        writeInt(storage, annotationRef, F_CONCURRENT_VALUE, N_CONCURRENT_VALUE, i);
                    }
                }));
            }
            for (var future : futures) {
                future.get();
            }
        }
        return System.nanoTime() - start;
    }

    private static int readInt(DUACasStorage storage, int fsRef, int featureCode, String featureName) {
        return storage.readSlot(fsRef, featureCode, featureName).map(DUACasValue::intValue).orElse(0);
    }

    private static void writeInt(DUACasStorage storage, int fsRef, int featureCode, String featureName, int value) {
        storage.writeSlot(fsRef, featureCode, featureName, DUACasValue.ofInt(value));
    }

    private static void writeRef(DUACasStorage storage, int fsRef, int featureCode, String featureName, int targetRef) {
        storage.writeSlot(fsRef, featureCode, featureName, DUACasValue.ref(targetRef));
    }

    private static int tieredHotSetCapacity(DUAUceStressProfile profile) {
        long corpusEntities = profile.documents()
                + profile.pageCount()
                + profile.annotationCount()
                + profile.metadataCount()
                + profile.semanticRoleEventCount()
                + profile.associationCount();
        return Math.toIntExact(Math.min(1_000_000L, Math.max(4_096L, corpusEntities)));
    }

    private static long usedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void forceGc() throws InterruptedException {
        System.gc();
        Thread.sleep(25);
    }

    private static final class SyntheticCorpus {
        private final DUAUceStressProfile profile;
        private final int[] documents;
        private final int[] pages;
        private final int[] annotations;
        private final int[] metadata;
        private final int[] semanticRoleEvents;
        private final int[] associations;

        private SyntheticCorpus(DUAUceStressProfile profile) {
            this.profile = profile;
            this.documents = new int[profile.documents()];
            this.pages = new int[Math.toIntExact(profile.pageCount())];
            this.annotations = new int[Math.toIntExact(profile.annotationCount())];
            this.metadata = new int[Math.toIntExact(profile.metadataCount())];
            this.semanticRoleEvents = new int[Math.toIntExact(profile.semanticRoleEventCount())];
            this.associations = new int[Math.toIntExact(profile.associationCount())];
        }
    }
}
