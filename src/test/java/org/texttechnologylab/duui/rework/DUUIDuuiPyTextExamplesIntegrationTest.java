package org.texttechnologylab.duui.rework;

import com.sun.net.httpserver.HttpServer;
import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XMLInputSource;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;
import org.texttechnologylab.duui.runtime.DUUISystemScope;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUIDuuiPyTextExamplesIntegrationTest {
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    @Test
    void gnfinderExampleProcessesText() throws Exception {
        JCas cas = run(
                endpoint("duui.py.gnfinder.endpoint", "http://127.0.0.1:19714"),
                "gnfinder",
                "Homo sapiens and Panthera leo live here.",
                Map.of("lang", "en", "verify", "true"));

        assertCovered(cas, "org.texttechnologylab.annotation.biofid.gnfinder.Taxon", "Homo sapiens", "Panthera leo");
    }

    @Test
    void taxonerdExampleProcessesText() throws Exception {
        JCas cas = run(
                endpoint("duui.py.taxonerd.endpoint", "http://127.0.0.1:19718"),
                "taxonerd",
                "Homo sapiens and Panthera leo live here.",
                Map.of("model", "en_ner_eco_md", "linking", "gbif_backbone"));

        assertCovered(cas, "org.texttechnologylab.annotation.type.Taxon", "Homo sapiens", "Panthera leo");
    }

    @Test
    void argumentExampleProcessesText() throws Exception {
        JCas cas = run(
                endpoint("duui.py.argument.endpoint", "http://127.0.0.1:19715"),
                "argument",
                "We should preserve biodiversity because it benefits ecosystems.",
                Map.of("topic", "biodiversity"));

        assertAnnotationCountAtLeast(cas, "org.texttechnologylab.annotation.Argument", 1);
    }

    @Test
    void essayScorerExampleProcessesText() throws Exception {
        JCas cas = run(
                endpoint("duui.py.essay.endpoint", "http://127.0.0.1:19716"),
                "essay",
                "This answer is coherent, specific, and supported by several examples.",
                Map.of("name_model", "heuristic-essay-scorer"));

        assertAnnotationCountAtLeast(cas, "org.texttechnologylab.annotation.EssayScore", 1);
    }

    @Test
    void srlExampleProcessesText() throws Exception {
        JCas cas = run(
                endpoint("duui.py.srl.endpoint", "http://127.0.0.1:19717"),
                "srl",
                "Researchers protect forests today.",
                Map.of("max_links_per_sentence", "3"));

        assertAnnotationCountAtLeast(cas, "org.texttechnologylab.annotation.semaf.isobase.Entity", 1);
        assertFsCountAtLeast(cas, "org.texttechnologylab.annotation.semaf.semafsr.SrLink", 1);
    }

    @Test
    void spacyExampleProcessesText() throws Exception {
        JCas cas = run(
                endpoint("duui.py.spacy.endpoint", "http://127.0.0.1:19719"),
                "spacy",
                "Frankfurt is a city. Biodiversity matters.",
                Map.of("model_name", "en_core_web_sm"));

        assertAnnotationCountAtLeast(cas, "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token", 1);
        assertAnnotationCountAtLeast(cas, "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence", 1);
    }

    @Test
    void geonamesExampleProcessesLocationAnnotationsThroughBackend() throws Exception {
        HttpServer backend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        backend.createContext("/", exchange -> {
            byte[] response = """
                    {"results":[{"reference":"1","entry":{"id":2925533,"name":"Frankfurt am Main","latitude":50.11552,"longitude":8.68417,"feature_class":"P","feature_code":"PPLA2","country_code":"DE","adm1":"05","adm2":"","adm3":"","adm4":"","elevation":null}}]}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        backend.start();
        try {
            URI endpoint = endpoint("duui.py.geonames.endpoint", "http://127.0.0.1:19720");
            JCas cas = casFor(endpoint, "Frankfurt am Main ist eine Stadt.");
            addAnnotation(cas, "de.tudarmstadt.ukp.dkpro.core.api.ner.type.Location", 0, 17);

            runThroughOrchestrator(
                    endpoint,
                    "geonames",
                    cas,
                    Map.of("backend_url", "http://127.0.0.1:" + backend.getAddress().getPort()));

            assertCovered(cas, "org.texttechnologylab.annotation.geonames.GeoNamesEntity", "Frankfurt am Main");
        } finally {
            backend.stop(0);
        }
    }

    private static JCas run(URI endpoint, String id, String text, Map<String, String> parameters) throws Exception {
        JCas cas = casFor(endpoint, text);
        runThroughOrchestrator(endpoint, id, cas, parameters);
        return cas;
    }

    private static void runThroughOrchestrator(URI endpoint, String id, JCas cas, Map<String, String> parameters) throws Exception {
        assertHealthy(endpoint);
        try (DUUISystemScope system = DUUI.system("duui-py-text-" + id)) {
            try (DUUIPipelineScope pipeline = system.pipeline(id + "-pipeline")) {
                try (DUUIGeneratorScope<JCas> documents = new SingleJCasSource(cas).open(pipeline)) {
                    try (DUUIStageScope<JCas> remote = documents.linear("remote-" + id)) {
                        remote.v1(id)
                                .remote()
                                .endpoint(endpoint.toString())
                                .sourceView("_InitialView")
                                .targetView("_InitialView")
                                .telemetry()
                                .parameters(parameters);
                    }
                }
            }
            DUUIOrchestrationResult result = system.run(id + "-pipeline");
            assertTrue(!result.hasFailures(), () -> describeFailures(result));
            assertEquals(0, result.unroutableArtifacts().size());
        }
    }

    private static URI endpoint(String property, String defaultValue) {
        return URI.create(System.getProperty(property, defaultValue));
    }

    private static JCas casFor(URI endpoint, String text) throws Exception {
        TypeSystemDescription typeSystem = mergedRemoteTypeSystem(endpoint);
        JCas cas = JCasFactory.createJCas(typeSystem);
        cas.setDocumentLanguage("de");
        cas.setDocumentText(text);
        return cas;
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

    private static String describeFailures(DUUIOrchestrationResult result) {
        StringBuilder builder = new StringBuilder();
        result.results().stream()
                .filter(execution -> execution.failure() != null)
                .forEach(execution -> builder.append(execution.failure().message())
                        .append(" cause=")
                        .append(execution.failure().cause())
                        .append('\n'));
        return builder.toString();
    }

    private record SingleJCasSource(JCas cas) implements DUUIGenerator<JCas> {
        @Override
        public void generate(DUUIArtifactEmitter<JCas> emitter) {
            emitter.emit(DUUIArtifact.of(cas));
        }
    }

    private static TypeSystemDescription mergedRemoteTypeSystem(URI endpoint) throws Exception {
        HttpResponse<String> response = CLIENT.send(
                HttpRequest.newBuilder(endpoint.resolve("/v1/typesystem"))
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertEquals(200, response.statusCode(), () -> endpoint + " typesystem was not available");
        TypeSystemDescription remote = UIMAFramework.getXMLParser().parseTypeSystemDescription(
                new XMLInputSource(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8)), null));
        return CasCreationUtils.mergeTypeSystems(List.of(
                TypeSystemDescriptionFactory.createTypeSystemDescription(),
                remote
        ));
    }

    private static void addAnnotation(JCas cas, String typeName, int begin, int end) {
        CAS view = cas.getCas();
        Type type = view.getTypeSystem().getType(typeName);
        assertNotNull(type, () -> "Missing input type " + typeName);
        AnnotationFS annotation = view.createAnnotation(type, begin, end);
        view.addFsToIndexes(annotation);
    }

    private static void assertCovered(JCas cas, String typeName, String... expectedCoveredText) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        Type type = view.getTypeSystem().getType(typeName);
        assertNotNull(type, () -> "Missing output type " + typeName);
        List<String> coveredText = new java.util.ArrayList<>();
        for (AnnotationFS annotation : view.getAnnotationIndex(type)) {
            coveredText.add(annotation.getCoveredText());
        }
        for (String expected : expectedCoveredText) {
            assertTrue(coveredText.contains(expected), () -> "Missing " + expected + " for " + typeName + " in " + coveredText);
        }
    }

    private static void assertAnnotationCountAtLeast(JCas cas, String typeName, int minimum) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        Type type = view.getTypeSystem().getType(typeName);
        assertNotNull(type, () -> "Missing output type " + typeName);
        int count = 0;
        for (AnnotationFS ignored : view.getAnnotationIndex(type)) {
            count++;
        }
        int observed = count;
        assertTrue(observed >= minimum, () -> "Expected at least " + minimum + " annotations of " + typeName + ", got " + observed);
    }

    private static void assertFsCountAtLeast(JCas cas, String typeName, int minimum) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        Type type = view.getTypeSystem().getType(typeName);
        assertNotNull(type, () -> "Missing output type " + typeName);
        int count = view.select(type).asList().size();
        assertTrue(count >= minimum, () -> "Expected at least " + minimum + " feature structures of " + typeName + ", got " + count);
    }
}
