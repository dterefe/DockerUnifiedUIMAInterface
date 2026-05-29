package org.texttechnologylab.duui.rework;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XMLInputSource;
import org.junit.jupiter.api.Test;
import org.json.JSONArray;
import org.json.JSONObject;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIInMemoryEventSink;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;
import org.texttechnologylab.duui.runtime.DUUISystemScope;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DUUIDashboardGenericPipelineTest {
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    @Test
    void runDashboardConfiguredPipeline() throws Exception {
        JSONArray stages = new JSONArray(System.getProperty("duui.dashboard.stages", "[]"));
        if (stages.length() == 0) {
            throw new IllegalArgumentException("duui.dashboard.stages must contain at least one remote DUUI stage");
        }
        List<URI> endpoints = new ArrayList<>();
        for (int i = 0; i < stages.length(); i++) {
            endpoints.add(URI.create(stages.getJSONObject(i).getString("endpoint")));
        }
        for (URI endpoint : endpoints) {
            assertHealthy(endpoint);
        }
        TypeSystemDescription typeSystem = mergedRemoteTypeSystem(endpoints);
        List<DocumentCase> documents = documents(typeSystem);
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();
        DUUIEventService events = new DUUIEventService(List.of(sink));

        long started = System.nanoTime();
        try (DUUISystemScope system = DUUI.system("dashboard-generic-pipeline").events(events)) {
            try (DUUIPipelineScope pipeline = system.pipeline("dashboard-pipeline")) {
                try (DUUIGeneratorScope<JCas> source = new JCasSource(documents.stream().map(DocumentCase::cas).toList()).open(pipeline)) {
                    for (int i = 0; i < stages.length(); i++) {
                        JSONObject stageSpec = stages.getJSONObject(i);
                        String stageId = stageSpec.optString("id", "stage-" + (i + 1));
                        try (DUUIStageScope<JCas> stage = source.linear(stageId)) {
                            String dispatch = stageSpec.optString("dispatch", "platform");
                            if ("virtual".equalsIgnoreCase(dispatch) || "io".equalsIgnoreCase(dispatch)) {
                                stage.dispatchPolicy(DUUIDispatchPolicy.of(DUUIDispatchMode.IO, stageSpec.optInt("parallelism", 8)));
                            } else if ("cpu".equalsIgnoreCase(dispatch)) {
                                stage.dispatchPolicy(DUUIDispatchPolicy.of(DUUIDispatchMode.CPU, stageSpec.optInt("parallelism", Runtime.getRuntime().availableProcessors())));
                            }
                            var component = stage.v1(stageId)
                                    .remote()
                                    .endpoint(stageSpec.getString("endpoint"))
                                    .sourceView(stageSpec.optString("sourceView", "_InitialView"))
                                    .targetView(stageSpec.optString("targetView", "_InitialView"))
                                    .telemetrySink(sink)
                                    .parameters(parameters(stageSpec.optJSONObject("parameters")));
                            component.streamingTransport(stageSpec.optBoolean("streaming", true));
                            if (stageSpec.has("contentType")) {
                                component.contentType(stageSpec.getString("contentType"));
                            }
                        }
                    }
                }
            }
            DUUIOrchestrationResult result = system.run("dashboard-pipeline");
            assertFalse(result.hasFailures(), () -> describeFailures(result));
            assertEquals(0, result.unroutableArtifacts().size());
        }
        long wallMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
        for (DocumentCase document : documents) {
            int annotationCount = document.cas().getView("_InitialView").getAnnotationIndex().size();
            System.out.printf(
                    "DASHBOARD_PIPELINE_RESULT document=%s stages=%d elapsed_ms=%d annotations=%d types=%s%n",
                    sanitize(document.name()),
                    stages.length(),
                    wallMs,
                    annotationCount,
                    typeSummary(document.cas())
            );
        }
    }

    private static Map<String, String> parameters(JSONObject value) {
        Map<String, String> out = new LinkedHashMap<>();
        if (value == null) return out;
        for (String key : value.keySet()) {
            out.put(key, String.valueOf(value.get(key)));
        }
        return out;
    }

    private static List<DocumentCase> documents(TypeSystemDescription typeSystem) throws Exception {
        String sampleFiles = System.getProperty("duui.dashboard.sample.files", "").trim();
        int docs = Math.max(1, Integer.getInteger("duui.dashboard.docs", 1));
        List<DocumentCase> out = new ArrayList<>();
        if (!sampleFiles.isBlank()) {
            for (String raw : sampleFiles.split(",")) {
                if (raw.isBlank()) continue;
                JCas cas = JCasFactory.createJCas(typeSystem);
                loadXmi(cas, raw.trim());
                out.add(new DocumentCase(raw.substring(raw.lastIndexOf('/') + 1), cas));
                if (out.size() >= docs) break;
            }
            return out;
        }
        String text = System.getProperty("duui.dashboard.text", "Die Gemeine Fichte wächst in Deutschland.");
        String language = System.getProperty("duui.dashboard.language", "de");
        for (int i = 0; i < docs; i++) {
            JCas cas = JCasFactory.createJCas(typeSystem);
            cas.setDocumentLanguage(language);
            cas.setDocumentText(text);
            out.add(new DocumentCase("text-" + (i + 1), cas));
        }
        return out;
    }

    private static void loadXmi(JCas cas, String path) throws Exception {
        try (InputStream file = new FileInputStream(path);
             InputStream input = path.endsWith(".gz") ? new GZIPInputStream(file) : file) {
            XmiCasDeserializer.deserialize(input, cas.getCas(), true);
        }
    }

    private static void assertHealthy(URI endpoint) throws Exception {
        HttpResponse<Void> response = CLIENT.send(
                HttpRequest.newBuilder(endpoint.resolve("/v1/documentation"))
                        .timeout(Duration.ofSeconds(8))
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
                            .timeout(Duration.ofSeconds(8))
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

    private static String typeSummary(JCas cas) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        List<String> types = List.of(
                "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token",
                "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence",
                "de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity",
                "org.texttechnologylab.annotation.type.Taxon",
                "org.texttechnologylab.annotation.biofid.gnfinder.GNFinderNamedEntity"
        );
        List<String> parts = new ArrayList<>();
        for (String typeName : types) {
            Type type = view.getTypeSystem().getType(typeName);
            if (type == null) continue;
            int count = countIndexed(view, type);
            if (count > 0) {
                parts.add(typeName.substring(typeName.lastIndexOf('.') + 1) + ":" + count);
            }
        }
        return String.join("|", parts);
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

    private static String sanitize(String value) {
        return value.replaceAll("\\s+", "_").replace("=", "-");
    }

    private record DocumentCase(String name, JCas cas) {
    }

    private record JCasSource(List<JCas> cases) implements DUUIGenerator<JCas> {
        @Override
        public void generate(DUUIArtifactEmitter<JCas> emitter) {
            for (JCas cas : cases) {
                emitter.emit(DUUIArtifact.of(cas));
            }
        }
    }
}
