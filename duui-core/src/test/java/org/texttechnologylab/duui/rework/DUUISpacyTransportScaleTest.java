package org.texttechnologylab.duui.rework;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Type;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XMLInputSource;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.event.DUUIEvent;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIInMemoryEventSink;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;
import org.texttechnologylab.duui.runtime.DUUISystemScope;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DUUISpacyTransportScaleTest {
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    @Test
    void compareBufferedAndStreamingTransportWithGrowingSpacyAnnotations() throws Exception {
        URI endpoint = URI.create(System.getProperty("duui.py.spacy.endpoint", "http://spacy:9714"));
        assertHealthy(endpoint);
        TypeSystemDescription typeSystem = mergedRemoteTypeSystem(List.of(endpoint));

        // Warm the spaCy model and the remote type-system path before collecting scale numbers.
        runSpacy(endpoint, typeSystem, "warm-buffer", text(10), false);
        runSpacy(endpoint, typeSystem, "warm-stream", text(10), true);

        List<Result> results = new ArrayList<>();
        for (int sentences : scaleSentences()) {
            String text = text(sentences);
            Result bufferedA = runSpacy(endpoint, typeSystem, "buffer-a-" + sentences, text, false);
            Result streamingA = runSpacy(endpoint, typeSystem, "stream-a-" + sentences, text, true);
            Result streamingB = runSpacy(endpoint, typeSystem, "stream-b-" + sentences, text, true);
            Result bufferedB = runSpacy(endpoint, typeSystem, "buffer-b-" + sentences, text, false);
            Result buffered = faster(bufferedA, bufferedB);
            Result streaming = faster(streamingA, streamingB);
            results.add(buffered);
            results.add(streaming);
            assertEquals(buffered.annotationCount(), streaming.annotationCount(), "annotation counts differ at sentences=" + sentences);
            System.out.printf(
                    "SPACY_TRANSPORT_SCALE sentences=%d chars=%d annotations=%d buffer_best_ms=%d stream_best_ms=%d delta_ms=%d buffer_decode_ms=%.1f stream_overlap_ms=%.1f buffer_receive_ms=%.1f response_bytes=%.0f%n",
                    sentences,
                    text.length(),
                    buffered.annotationCount(),
                    buffered.totalMs(),
                    streaming.totalMs(),
                    buffered.totalMs() - streaming.totalMs(),
                    buffered.metric("duui.http.response_decode_ms"),
                    streaming.metric("duui.http.response_decode_ms"),
                    buffered.metric("duui.http.response_receive_ms"),
                    buffered.metric("duui.http.response_bytes")
            );
        }

        Result largestBuffered = results.stream()
                .filter(result -> !result.streaming())
                .max(Comparator.comparing(Result::annotationCount))
                .orElseThrow();
        Result largestStreaming = results.stream()
                .filter(Result::streaming)
                .max(Comparator.comparing(Result::annotationCount))
                .orElseThrow();
        assertFalse(largestBuffered.annotationCount() < 1 || largestStreaming.annotationCount() < 1, "spaCy did not write annotations");
    }

    @Test
    void compareOldDuuiJsonLuaAndModernMsgpackLuaWithGrowingSpacyAnnotations() throws Exception {
        URI legacy = URI.create(System.getProperty("duui.py.spacy.legacy.endpoint", "http://duui-spacy-old-test:9714"));
        URI modern = URI.create(System.getProperty("duui.py.spacy.endpoint", "http://spacy:9714"));
        assertHealthy(legacy);
        assertHealthy(modern);
        TypeSystemDescription typeSystem = mergedRemoteTypeSystem(List.of(legacy, modern));

        runSpacy(legacy, typeSystem, "legacy-warm", text(10), false);
        runSpacy(modern, typeSystem, "modern-warm", text(10), true);

        for (int sentences : scaleSentences()) {
            String text = text(sentences);
            Result legacyA = runSpacy(legacy, typeSystem, "legacy-a-" + sentences, text, false);
            Result modernA = runSpacy(modern, typeSystem, "modern-a-" + sentences, text, true);
            Result modernB = runSpacy(modern, typeSystem, "modern-b-" + sentences, text, true);
            Result legacyB = runSpacy(legacy, typeSystem, "legacy-b-" + sentences, text, false);
            Result legacyBest = faster(legacyA, legacyB);
            Result modernBest = faster(modernA, modernB);
            System.out.printf(
                    "SPACY_OLD_DUUI_VS_MSGPACK_SCALE sentences=%d chars=%d old_duui_annotations=%d modern_annotations=%d old_duui_best_ms=%d modern_best_ms=%d delta_ms=%d old_duui_decode_ms=%.1f modern_overlap_ms=%.1f old_duui_receive_ms=%.1f old_duui_response_bytes=%.0f modern_response_bytes=%.0f old_duui_types=%s modern_types=%s%n",
                    sentences,
                    text.length(),
                    legacyBest.annotationCount(),
                    modernBest.annotationCount(),
                    legacyBest.totalMs(),
                    modernBest.totalMs(),
                    legacyBest.totalMs() - modernBest.totalMs(),
                    legacyBest.metric("duui.http.response_decode_ms"),
                    modernBest.metric("duui.http.response_decode_ms"),
                    legacyBest.metric("duui.http.response_receive_ms"),
                    legacyBest.metric("duui.http.response_bytes"),
                    modernBest.metric("duui.http.response_bytes"),
                    legacyBest.typeSummary(),
                    modernBest.typeSummary()
            );
            assertFalse(legacyBest.annotationCount() < 1 || modernBest.annotationCount() < 1, "spaCy did not write annotations");
        }
    }

    @Test
    void compareStreamingPlatformAndVirtualExecutorsSingleAndMultiDocument() throws Exception {
        URI endpoint = URI.create(System.getProperty("duui.py.spacy.endpoint", "http://spacy:9714"));
        assertHealthy(endpoint);
        TypeSystemDescription typeSystem = mergedRemoteTypeSystem(List.of(endpoint));

        runSpacy(endpoint, typeSystem, "executor-warm-platform", text(10), true, ExecutorMode.PLATFORM);
        runSpacy(endpoint, typeSystem, "executor-warm-virtual", text(10), true, ExecutorMode.VIRTUAL);

        int multiDocs = Integer.getInteger("duui.py.spacy.executor.multiDocs", 8);
        for (int sentences : scaleSentences()) {
            String text = text(sentences);
            for (int docs : List.of(1, multiDocs)) {
                Result platformA = runSpacyBatch(endpoint, typeSystem, "executor-platform-a-" + docs + "-" + sentences, text, docs, true, ExecutorMode.PLATFORM);
                Result virtualA = runSpacyBatch(endpoint, typeSystem, "executor-virtual-a-" + docs + "-" + sentences, text, docs, true, ExecutorMode.VIRTUAL);
                Result virtualB = runSpacyBatch(endpoint, typeSystem, "executor-virtual-b-" + docs + "-" + sentences, text, docs, true, ExecutorMode.VIRTUAL);
                Result platformB = runSpacyBatch(endpoint, typeSystem, "executor-platform-b-" + docs + "-" + sentences, text, docs, true, ExecutorMode.PLATFORM);
                Result platform = faster(platformA, platformB);
                Result virtual = faster(virtualA, virtualB);
                assertEquals(platform.annotationCount(), virtual.annotationCount(), "annotation counts differ docs=" + docs + " sentences=" + sentences);
                System.out.printf(
                        "SPACY_EXECUTOR_MATRIX protocol=runtime-msgpack-windowed java_transport=v1-streaming document_mode=%s docs=%d sentences=%d chars_per_doc=%d annotations=%d platform_ms=%d virtual_ms=%d delta_ms=%d platform_decode_ms=%.1f virtual_decode_ms=%.1f platform_bytes=%.0f virtual_bytes=%.0f platform_types=%s virtual_types=%s%n",
                        docs == 1 ? "single-doc" : "multi-doc",
                        docs,
                        sentences,
                        text.length(),
                        platform.annotationCount(),
                        platform.totalMs(),
                        virtual.totalMs(),
                        platform.totalMs() - virtual.totalMs(),
                        platform.metric("duui.http.response_decode_ms"),
                        virtual.metric("duui.http.response_decode_ms"),
                        platform.metric("duui.http.response_bytes"),
                        virtual.metric("duui.http.response_bytes"),
                        platform.typeSummary(),
                        virtual.typeSummary()
                );
            }
        }
    }

    private static Result runSpacy(URI endpoint, TypeSystemDescription typeSystem, String id, String text, boolean streaming) throws Exception {
        return runSpacy(endpoint, typeSystem, id, text, streaming, ExecutorMode.PLATFORM);
    }

    private static Result runSpacy(URI endpoint, TypeSystemDescription typeSystem, String id, String text, boolean streaming, ExecutorMode executorMode) throws Exception {
        return runSpacyBatch(endpoint, typeSystem, id, text, 1, streaming, executorMode);
    }

    private static Result runSpacyBatch(URI endpoint, TypeSystemDescription typeSystem, String id, String text, int documents, boolean streaming, ExecutorMode executorMode) throws Exception {
        List<JCas> cases = new ArrayList<>();
        for (int i = 0; i < documents; i++) {
            cases.add(jcas(typeSystem, text));
        }
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService events = new DUUIEventService(List.of(sink));
        long started = System.nanoTime();
        try (DUUISystemScope system = DUUI.system("spacy-transport-" + id).events(events)) {
            try (DUUIPipelineScope pipeline = system.pipeline(id + "-pipeline")) {
                try (DUUIGeneratorScope<JCas> documentsScope = new JCasListSource(cases).open(pipeline)) {
                    try (DUUIStageScope<JCas> remote = documentsScope.linear("remote-" + id)) {
                        if (executorMode == ExecutorMode.VIRTUAL) {
                            remote.dispatchPolicy(DUUIDispatchPolicy.of(DUUIDispatchMode.IO, Integer.getInteger("duui.py.spacy.executor.virtualParallelism", 8)));
                        }
                        var component = remote.v1(id)
                                .remote()
                                .endpoint(endpoint.toString())
                                .sourceView("_InitialView")
                                .targetView("_InitialView")
                                .telemetrySink(sink)
                                .parameters(Map.of(
                                        "model_name", System.getProperty("duui.py.spacy.model", "de_core_news_sm"),
                                        "spacy_language", "de"
                                ));
                        component.streamingTransport(streaming);
                        if (id.startsWith("legacy-")) {
                            component.contentType("application/json");
                        }
                    }
                }
            }
            DUUIOrchestrationResult result = system.run(id + "-pipeline");
            assertFalse(result.hasFailures(), () -> describeFailures(result));
            assertEquals(0, result.unroutableArtifacts().size());
        }
        long totalMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
        return new Result(id, streaming, totalMs, cases.stream().mapToInt(cas -> {
            try {
                return countSpaCyAnnotations(cas);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).sum(), mergeTypeCounts(cases), sink.events());
    }

    private static JCas jcas(TypeSystemDescription typeSystem, String text) throws Exception {
        JCas cas = JCasFactory.createJCas(typeSystem);
        cas.setDocumentLanguage("de");
        cas.setDocumentText(text);
        return cas;
    }

    private static int countSpaCyAnnotations(JCas cas) throws Exception {
        return spaCyAnnotationSummary(cas).values().stream().mapToInt(Integer::intValue).sum();
    }

    private static Map<String, Integer> spaCyAnnotationSummary(JCas cas) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        Map<String, Integer> counts = new java.util.LinkedHashMap<>();
        for (String typeName : List.of(
                "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token",
                "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence",
                "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma",
                "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS",
                "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.morph.MorphologicalFeatures",
                "de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency",
                "de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity",
                "org.texttechnologylab.uima.type.spacy.SpacyToken",
                "org.texttechnologylab.uima.type.spacy.SpacyNounChunk",
                "org.texttechnologylab.annotation.SpacyAnnotatorMetaData",
                "org.texttechnologylab.annotation.DocumentModification",
                "org.texttechnologylab.type.id.URL"
        )) {
            Type type = view.getTypeSystem().getType(typeName);
            if (type != null) {
                int count = countIndexed(view, type);
                if (count > 0) {
                    counts.put(typeName.substring(typeName.lastIndexOf('.') + 1), count);
                }
            }
        }
        return counts;
    }

    private static Map<String, Integer> mergeTypeCounts(List<JCas> cases) throws Exception {
        Map<String, Integer> merged = new java.util.LinkedHashMap<>();
        for (JCas cas : cases) {
            for (Map.Entry<String, Integer> entry : spaCyAnnotationSummary(cas).entrySet()) {
                merged.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }
        return merged;
    }

    private static int countIndexed(CAS view, Type type) {
        int count = 0;
        FSIterator<FeatureStructure> iterator = view.getIndexRepository().getAllIndexedFS(type);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    private static String text(int sentences) {
        String sentence = "Die Gemeine Fichte waechst in Waeldern, und Forscher untersuchen trockene Sommer in Deutschland. ";
        return sentence.repeat(sentences);
    }

    private static List<Integer> scaleSentences() {
        String configured = System.getProperty("duui.py.spacy.scale.sentences", "25,100,250,1000");
        return java.util.Arrays.stream(configured.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .map(Integer::parseInt)
                .toList();
    }

    private static Result faster(Result first, Result second) {
        return first.totalMs() <= second.totalMs() ? first : second;
    }

    private static void assertHealthy(URI endpoint) throws Exception {
        HttpResponse<Void> response = CLIENT.send(
                HttpRequest.newBuilder(endpoint.resolve("/v1/documentation"))
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.discarding());
        assertEquals(200, response.statusCode(), () -> endpoint + " is not healthy");
    }

    private static TypeSystemDescription mergedRemoteTypeSystem(List<URI> endpoints) throws Exception {
        List<TypeSystemDescription> descriptions = new ArrayList<>();
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        for (URI endpoint : endpoints) {
            HttpResponse<String> response = CLIENT.send(
                    HttpRequest.newBuilder(endpoint.resolve("/v1/typesystem"))
                            .timeout(Duration.ofSeconds(5))
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            assertEquals(200, response.statusCode(), () -> endpoint + " typesystem was not available");
            descriptions.add(UIMAFramework.getXMLParser().parseTypeSystemDescription(
                    new XMLInputSource(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8)), null)));
        }
        return CasCreationUtils.mergeTypeSystems(descriptions);
    }

    private static String describeFailures(DUUIOrchestrationResult result) {
        StringBuilder builder = new StringBuilder();
        result.results().stream()
                .map(DUUIExecutionResult::failure)
                .filter(Objects::nonNull)
                .forEach(failure -> builder.append(failure.message())
                        .append(" cause=")
                        .append(failure.cause())
                        .append('\n'));
        return builder.toString();
    }

    private record Result(String id, boolean streaming, long totalMs, int annotationCount, Map<String, Integer> typeCounts,
                          List<DUUIEvent> events) {
        double metric(String name) {
            return events.stream()
                    .filter(event -> name.equals(event.metricName()))
                    .map(DUUIEvent::metricValue)
                    .filter(Objects::nonNull)
                    .reduce((first, second) -> second)
                    .orElse(Double.NaN);
        }

        String typeSummary() {
            return typeCounts.entrySet().stream()
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(java.util.stream.Collectors.joining("|"));
        }
    }

    private enum ExecutorMode {
        PLATFORM,
        VIRTUAL
    }

    private record JCasListSource(List<JCas> cases) implements DUUIGenerator<JCas> {
        @Override
        public void generate(DUUIArtifactEmitter<JCas> emitter) {
            for (JCas cas : cases) {
                emitter.emit(DUUIArtifact.of(cas));
            }
        }
    }
}
