package org.texttechnologylab.duui.rework;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.XMLInputSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIEventSink;
import org.texttechnologylab.duui.event.DUUIEventSinks;
import org.texttechnologylab.duui.event.DUUIInMemoryEventSink;
import org.texttechnologylab.duui.orchestration.DUUIDispatchMode;
import org.texttechnologylab.duui.orchestration.DUUIDispatchPolicy;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.pipeline.io.DUUIXmiCollectionReader;
import org.texttechnologylab.duui.pipeline.io.DUUIXmiTarget;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;
import org.texttechnologylab.duui.runtime.DUUISystemScope;
import org.texttechnologylab.duui.runtime.DUUIV1ComponentBuilder;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUIDistributedBiofidPipelineTest {
    private static final DUUIArtifactType<JCas> BIOFID_JCAS = DUUIArtifactType.of("biofid/distributed/jcas");
    private static final String EXAMPLES = "../duui-py/examples";

    @TempDir
    Path tempDir;

    @Test
    @EnabledIfSystemProperty(named = "duui.distributed.enabled", matches = "true")
    void biofidStylePipelineRunsOnConfiguredDistributedEnvironment() throws Exception {
        DistributedMode mode = DistributedMode.valueOf(System.getProperty("duui.distributed.mode", "REMOTE").toUpperCase());
        TypeSystemDescription typeSystem = localExampleTypeSystem();
        Path input = tempDir.resolve("input");
        Path output = tempDir.resolve("output");
        Files.createDirectories(input);
        writeXmi(typeSystem, input.resolve("doc-1.xmi"), "Homo sapiens was found near Berlin. Quercus robur grows nearby.");
        writeXmi(typeSystem, input.resolve("doc-2.xmi"), "Panthera leo and Rosa canina occur in the report from Paris.");

        DUUIInMemoryEventSink memory = new DUUIInMemoryEventSink();
        DUUIEventSink sink = event -> {
            memory.accept(event);
            DUUIEventSinks.console().accept(event);
        };
        DUUIEventService events = new DUUIEventService(List.of(sink));
        DUUIDispatchPolicy ioDispatch = DUUIDispatchPolicy.of(
                DUUIDispatchMode.IO,
                Integer.getInteger("duui.distributed.dispatch.parallelism", 256)
        );

        DUUIOrchestrationResult result;
        try (GeoNamesBackend geonamesBackend = GeoNamesBackend.start();
             DUUISystemScope system = DUUI.system("biofid-distributed-" + mode.name().toLowerCase()).events(events)) {
            try (DUUIPipelineScope pipeline = system.pipeline("biofid-distributed")) {
                try (DUUIGeneratorScope<JCas> documents = DUUIXmiCollectionReader.builder()
                        .artifactType(BIOFID_JCAS)
                        .typeSystem(typeSystem)
                        .source(input)
                        .open(pipeline)) {
                    try (DUUIStageScope<JCas> spacy = documents.linear("spacy")) {
                        spacy.dispatchPolicy(ioDispatch);
                        component(mode, spacy, ComponentSpec.spacy(), sink, geonamesBackend);
                    }
                    try (DUUIStageScope<JCas> geonames = documents.linear("geonames")) {
                        geonames.dispatchPolicy(ioDispatch);
                        component(mode, geonames, ComponentSpec.geonames(), sink, geonamesBackend);
                    }
                    try (DUUIStageScope<JCas> gnfinder = documents.linear("gnfinder")) {
                        gnfinder.dispatchPolicy(ioDispatch);
                        component(mode, gnfinder, ComponentSpec.gnfinder(), sink, geonamesBackend);
                    }
                    try (DUUIStageScope<JCas> taxonerd = documents.linear("taxonerd")) {
                        taxonerd.dispatchPolicy(ioDispatch);
                        component(mode, taxonerd, ComponentSpec.taxonerd(), sink, geonamesBackend);
                    }
                    try (var ignored = DUUIXmiTarget.builder()
                            .artifactType(BIOFID_JCAS)
                            .output(output)
                            .open(documents)) {
                        // target scope is registered by construction
                    }
                }
            }
            result = system.run("biofid-distributed");
        }

        assertFalse(result.hasFailures(), () -> result.results().toString());
        assertTrue(Files.exists(output.resolve("doc-1.xmi")));
        assertTrue(Files.exists(output.resolve("doc-2.xmi")));
        assertFalse(memory.events().isEmpty(), "No DUUI telemetry events were recorded");
    }

    private static void component(DistributedMode mode, DUUIStageScope<JCas> stage, ComponentSpec spec, DUUIEventSink sink, GeoNamesBackend geonamesBackend) {
        DUUIV1ComponentBuilder builder = stage.v1(spec.id())
                .sourceView("_InitialView")
                .targetView("_InitialView")
                .scale(Integer.getInteger("duui.distributed." + spec.id() + ".scale", spec.defaultScale()))
                .concurrency(Integer.getInteger("duui.distributed." + spec.id() + ".concurrency", spec.defaultConcurrency()))
                .timeoutSeconds(Long.getLong("duui.distributed." + spec.id() + ".timeout.seconds", 3600L))
                .telemetrySink(sink)
                .parameters(parameters(spec, geonamesBackend));

        switch (mode) {
            case REMOTE -> builder.remote().endpoint(requiredProperty("duui.distributed." + spec.id() + ".endpoint"));
            case PODMAN -> builder.podman()
                    .image(System.getProperty("duui.distributed." + spec.id() + ".image", spec.localImage()))
                    .imageFetching(Boolean.getBoolean("duui.distributed.image.fetching"));
            case KUBERNETES -> builder.kubernetes()
                    .image(System.getProperty("duui.distributed." + spec.id() + ".image", spec.localImage()))
                    .labels(labels("duui.distributed.kubernetes.labels"));
        }
    }

    private static Map<String, String> parameters(ComponentSpec spec, GeoNamesBackend geonamesBackend) {
        Map<String, String> parameters = new HashMap<>(spec.parameters());
        if ("geonames".equals(spec.id())) {
            parameters.put("backend_url", System.getProperty("duui.distributed.geonames.backend_url", geonamesBackend.url()));
        }
        return parameters;
    }

    private static String requiredProperty(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required system property: " + key);
        }
        assertHealthy(URI.create(value));
        return value;
    }

    private static void assertHealthy(URI endpoint) {
        try {
            HttpResponse<Void> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(endpoint.resolve("/v1/documentation"))
                            .timeout(Duration.ofSeconds(5))
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() != 200) {
                throw new IllegalStateException(endpoint + " returned " + response.statusCode());
            }
        } catch (Exception e) {
            throw new IllegalStateException("DUUI v1 endpoint is not healthy: " + endpoint, e);
        }
    }

    private static List<String> labels(String key) {
        String value = System.getProperty(key, "");
        if (value.isBlank()) {
            return List.of();
        }
        return List.of(value.split(","));
    }

    private static TypeSystemDescription localExampleTypeSystem() throws Exception {
        List<TypeSystemDescription> descriptions = new ArrayList<>();
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        descriptions.add(typeSystem("spacy-lua-msgpack/TypeSystemSpacy.xml"));
        descriptions.add(typeSystem("geonames-msgpack-lua/TypeSystemGeoNames.xml"));
        descriptions.add(typeSystem("gnfinder-msgpack-lua/TypeSystemGNFinder.xml"));
        descriptions.add(typeSystem("taxonerd-msgpack-lua/TypeSystemTaxoNERD.xml"));
        return CasCreationUtils.mergeTypeSystems(descriptions);
    }

    private static TypeSystemDescription typeSystem(String relative) throws Exception {
        return UIMAFramework.getXMLParser().parseTypeSystemDescription(new XMLInputSource(Path.of(EXAMPLES, relative).toFile()));
    }

    private static void writeXmi(TypeSystemDescription typeSystem, Path path, String text) throws Exception {
        JCas cas = JCasFactory.createJCas(typeSystem);
        cas.setDocumentLanguage("en");
        cas.setDocumentText(text);
        try (OutputStream output = Files.newOutputStream(path)) {
            CasIOUtils.save(cas.getCas(), output, SerialFormat.XMI_1_1_PRETTY);
        }
    }

    private enum DistributedMode {
        REMOTE,
        PODMAN,
        KUBERNETES
    }

    private record ComponentSpec(
            String id,
            String localImage,
            int defaultScale,
            int defaultConcurrency,
            Map<String, String> parameters
    ) {
        static ComponentSpec spacy() {
            return new ComponentSpec(
                    "spacy",
                    "localhost/duui-py-spacy-lua-msgpack:dev",
                    2,
                    16,
                    Map.of("model_name", "en_core_web_sm")
            );
        }

        static ComponentSpec geonames() {
            return new ComponentSpec(
                    "geonames",
                    "localhost/duui-py-geonames-msgpack-lua:dev",
                    2,
                    16,
                    Map.of()
            );
        }

        static ComponentSpec gnfinder() {
            return new ComponentSpec(
                    "gnfinder",
                    "localhost/duui-py-gnfinder-msgpack-lua:dev",
                    2,
                    16,
                    Map.of("lang", "en", "verify", "true")
            );
        }

        static ComponentSpec taxonerd() {
            return new ComponentSpec(
                    "taxonerd",
                    "localhost/duui-py-taxonerd-msgpack-lua:dev",
                    2,
                    16,
                    Map.of("model", "en_ner_eco_md", "linking", "gbif_backbone", "threshold", "0.7")
            );
        }
    }

    private static final class GeoNamesBackend implements AutoCloseable {
        private final HttpServer server;
        private final String url;

        private GeoNamesBackend(HttpServer server) {
            this.server = server;
            String host = System.getProperty("duui.distributed.geonames.backend_host", "host.containers.internal");
            this.url = "http://" + host + ":" + server.getAddress().getPort();
        }

        static GeoNamesBackend start() throws Exception {
            HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
            server.createContext("/", GeoNamesBackend::respond);
            server.start();
            return new GeoNamesBackend(server);
        }

        String url() {
            return url;
        }

        private static void respond(HttpExchange exchange) throws java.io.IOException {
            exchange.getRequestBody().readAllBytes();
            byte[] body = """
                    {"results":[{"reference":"1","entry":{"id":2950159,"name":"Berlin","latitude":52.52,"longitude":13.405,"feature_class":"P","feature_code":"PPLC","country_code":"DE","adm1":"16"}}]}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
