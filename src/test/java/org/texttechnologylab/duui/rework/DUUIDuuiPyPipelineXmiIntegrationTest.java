package org.texttechnologylab.duui.rework;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.XMLInputSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;
import org.texttechnologylab.duui.event.DUUIEventType;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIEventSink;
import org.texttechnologylab.duui.event.DUUIEventSinks;
import org.texttechnologylab.duui.event.DUUIInMemoryEventSink;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.pipeline.DUUILambda;
import org.texttechnologylab.duui.pipeline.io.DUUIXmiCollectionReader;
import org.texttechnologylab.duui.pipeline.io.DUUIXmiTarget;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;
import org.texttechnologylab.duui.runtime.DUUISystemScope;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUIDuuiPyPipelineXmiIntegrationTest {
    private static final String TAXON_TYPE = "org.texttechnologylab.annotation.biofid.gnfinder.Taxon";
    private static final DUUIArtifactType<JCas> XMI_JCAS = DUUIArtifactType.of("duui-py/gnfinder/xmi-jcas");

    @TempDir
    Path tempDir;

    @Test
    void scopedPipelineReadsXmisProcessesRemoteGnfinderAndWritesXmis() throws Exception {
        URI endpoint = URI.create(System.getProperty("duui.py.gnfinder.endpoint", "http://127.0.0.1:19714"));
        HttpClient client = HttpClient.newHttpClient();
        assertHealthy(endpoint, client);

        TypeSystemDescription typeSystem = mergedRemoteTypeSystem(endpoint, client);
        Path inputDirectory = tempDir.resolve("input");
        Path outputDirectory = tempDir.resolve("output");
        Files.createDirectories(inputDirectory);
        writeXmi(typeSystem, inputDirectory.resolve("doc-1.xmi"), "Homo sapiens and Panthera leo live here.");
        writeXmi(typeSystem, inputDirectory.resolve("doc-2.xmi"), "Quercus robur grows near Rosa canina.");

        List<JCas> processed = new ArrayList<>();
        DUUIInMemoryEventSink events = new DUUIInMemoryEventSink();
        DUUIEventSink console = DUUIEventSinks.console();
        DUUIEventSink showcase = event -> {
            events.accept(event);
            console.accept(event);
        };
        DUUIEventService eventService = new DUUIEventService(List.of(showcase));
        DUUIOrchestrationResult result;
        try (DUUISystemScope system = DUUI.system("duui-py-pipeline-e2e").events(eventService)) {
            try (DUUIPipelineScope pipeline = system.pipeline("xmi-gnfinder")) {
                try (DUUIGeneratorScope<JCas> documents = DUUIXmiCollectionReader.builder()
                        .artifactType(XMI_JCAS)
                        .typeSystem(typeSystem)
                        .source(inputDirectory)
                        .open(pipeline)) {
                    try (DUUIStageScope<JCas> remoteGnfinder = documents.linear("remote-gnfinder")) {
                        remoteGnfinder.v1("gnfinder")
                                .remote()
                                .endpoint(endpoint.toString())
                                .sourceView("_InitialView")
                                .targetView("_InitialView")
                                .scale(1)
                                .concurrency(2)
                                .telemetrySink(showcase)
                                .parameters(Map.of("lang", "en", "verify", "true"));
                    }
                    try (DUUIStageScope<JCas> collect = documents.linear("collect")) {
                        collect.lambda(new CollectProcessedJCas(processed));
                    }
                    try (var ignored = DUUIXmiTarget.builder()
                            .artifactType(XMI_JCAS)
                            .output(outputDirectory)
                            .open(documents)) {
                        // target scope is registered by construction
                    }
                }
            }
            result = system.run("xmi-gnfinder");
        }

        assertFalse(result.hasFailures(), () -> describeFailures(result));
        assertEquals(0, result.unroutableArtifacts().size());
        assertEquals(2, processed.size());
        assertTrue(Files.exists(outputDirectory.resolve("doc-1.xmi")));
        assertTrue(Files.exists(outputDirectory.resolve("doc-2.xmi")));
        assertTaxa(processed.get(0), "Homo sapiens", "Panthera leo");
        assertTaxa(processed.get(1), "Quercus robur", "Rosa canina");
        waitForRemoteEvents(events);
        assertTrue(events.events().stream().anyMatch(event -> event.type() == DUUIEventType.LOG && "duui.executor".equals(event.name())), "DUUI Java executor log event missing");
        assertTrue(events.events().stream().anyMatch(event -> event.type() == DUUIEventType.LOG && "remote-log".equals(event.name())), "remote log event missing");
        assertTrue(events.events().stream().anyMatch(event -> event.type() == DUUIEventType.LOG && "GNFinder regex scan configured".equals(event.message())), "remote annotator debug log missing");
        assertTrue(events.events().stream().anyMatch(event -> event.type() == DUUIEventType.METRIC), "remote metric event missing");
        assertTrue(events.events().stream().anyMatch(event -> "remote-log".equals(event.name()) && event.artifactId() != null), "remote event context did not carry artifact id");
    }

    private static void assertHealthy(URI endpoint, HttpClient client) throws Exception {
        HttpResponse<Void> response = client.send(
                HttpRequest.newBuilder(endpoint.resolve("/v1/documentation"))
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.discarding());
        assertEquals(200, response.statusCode(), "duui-py GNFinder endpoint is not healthy");
    }

    private static String describeFailures(DUUIOrchestrationResult result) {
        StringBuilder builder = new StringBuilder();
        result.results().stream()
                .filter(execution -> execution.failure() != null)
                .forEach(execution -> {
                    builder.append(execution.status())
                            .append(" stage=")
                            .append(execution.failure().stageId())
                            .append(" message=")
                            .append(execution.failure().message())
                            .append(" cause=")
                            .append(execution.failure().cause())
                            .append('\n');
                });
        return builder.toString();
    }

    private static TypeSystemDescription mergedRemoteTypeSystem(URI endpoint, HttpClient client) throws Exception {
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder(endpoint.resolve("/v1/typesystem"))
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertEquals(200, response.statusCode(), "duui-py GNFinder typesystem was not available");
        TypeSystemDescription remote = UIMAFramework.getXMLParser().parseTypeSystemDescription(
                new XMLInputSource(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8)), null));
        return CasCreationUtils.mergeTypeSystems(List.of(
                TypeSystemDescriptionFactory.createTypeSystemDescription(),
                remote
        ));
    }

    private static void writeXmi(TypeSystemDescription typeSystem, Path path, String text) throws Exception {
        JCas cas = JCasFactory.createJCas(typeSystem);
        cas.setDocumentLanguage("en");
        cas.setDocumentText(text);
        try (OutputStream output = Files.newOutputStream(path)) {
            CasIOUtils.save(cas.getCas(), output, org.apache.uima.cas.SerialFormat.XMI_1_1_PRETTY);
        }
    }

    private static void assertTaxa(JCas cas, String... expectedCoveredText) throws Exception {
        CAS view = cas.getView("_InitialView").getCas();
        Type type = view.getTypeSystem().getType(TAXON_TYPE);
        assertNotNull(type, "GNFinder taxon type is missing from the CAS");
        List<String> coveredText = new ArrayList<>();
        for (AnnotationFS annotation : view.getAnnotationIndex(type)) {
            coveredText.add(annotation.getCoveredText());
        }
        for (String expected : expectedCoveredText) {
            assertTrue(coveredText.contains(expected), () -> "Missing taxon " + expected + " in " + coveredText);
        }
    }

    private static void waitForRemoteEvents(DUUIInMemoryEventSink sink) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            boolean hasLog = sink.events().stream().anyMatch(event -> event.type() == DUUIEventType.LOG);
            boolean hasMetric = sink.events().stream().anyMatch(event -> event.type() == DUUIEventType.METRIC);
            if (hasLog && hasMetric) return;
            Thread.sleep(100);
        }
    }

    private record CollectProcessedJCas(List<JCas> processed) implements DUUILambda<JCas> {
        @Override
        public DUUIArtifactType<JCas> inputType() {
            return XMI_JCAS;
        }

        @Override
        public DUUIArtifact<JCas> process(DUUIArtifact<JCas> artifact) {
            processed.add(artifact.payload());
            return artifact;
        }
    }
}
