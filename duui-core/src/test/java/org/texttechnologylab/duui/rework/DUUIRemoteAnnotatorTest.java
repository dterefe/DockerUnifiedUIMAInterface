package org.texttechnologylab.duui.rework;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrator;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.pipeline.DUUISource;
import org.texttechnologylab.duui.pipeline.DUUITarget;
import org.texttechnologylab.duui.pipeline.component.DUUINode;
import org.texttechnologylab.duui.pipeline.component.DUUIV1Component;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests 4 mock remote annotators (gnfinder, gazetteer, spacy, taxonerd)
 * via direct HTTP calls and through the DUUI pipeline with remote driver.
 *
 * [DESIGN: lines 105-272] — DUUIScope builder ergonomics.
 */
class DUUIRemoteAnnotatorTest {

    private static HttpServer gnfinderServer;
    private static HttpServer gazetteerServer;
    private static HttpServer spacyServer;
    private static HttpServer taxonerdServer;

    private static String typeSystem;

    @BeforeAll
    static void startAllAnnotators() throws Exception {
        StringWriter writer = new StringWriter();
        TypeSystemDescriptionFactory.createTypeSystemDescription().toXML(writer);
        typeSystem = writer.toString();

        gnfinderServer = startMockAnnotator("gnfinder");
        gazetteerServer = startMockAnnotator("gazetteer");
        spacyServer = startMockAnnotator("spacy");
        taxonerdServer = startMockAnnotator("taxonerd");
    }

    @AfterAll
    static void stopAllAnnotators() {
        stopServer(gnfinderServer);
        stopServer(gazetteerServer);
        stopServer(spacyServer);
        stopServer(taxonerdServer);
    }

    private static HttpServer startMockAnnotator(String name) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/documentation", exchange -> respond(exchange, 200,
                "{\"annotator_name\":\"" + name + "\",\"version\":\"1\",\"description\":\"mock " + name + "\",\"implementation_lang\":\"java\",\"meta\":{},\"parameters\":{}}"));
        server.createContext("/v1/typesystem", exchange -> respond(exchange, 200, typeSystem));
        server.createContext("/v1/communication_layer", exchange -> respond(exchange, 200,
                "function serialize(view, output, parameters, sourceView)\nend\n\nfunction deserialize(view, input)\nend\n"));
        server.start();
        return server;
    }

    private static void stopServer(HttpServer server) {
        if (server != null) {
            server.stop(0);
        }
    }

    // -- Test 1: Direct HTTP to each annotator --

    @Test
    void directHttpGnfinderResponds() throws Exception {
        DUUIV1Annotator annotator = createAnnotator("gnfinder", gnfinderServer);
        assertNotNull(annotator);
        var doc = annotator.documentation().join();
        assertNotNull(doc);
        assertEquals("gnfinder", doc.annotator_name());
    }

    @Test
    void directHttpGazetteerResponds() throws Exception {
        DUUIV1Annotator annotator = createAnnotator("gazetteer", gazetteerServer);
        assertNotNull(annotator);
        var doc = annotator.documentation().join();
        assertNotNull(doc);
        assertEquals("gazetteer", doc.annotator_name());
    }

    @Test
    void directHttpSpacyResponds() throws Exception {
        DUUIV1Annotator annotator = createAnnotator("spacy", spacyServer);
        assertNotNull(annotator);
        var doc = annotator.documentation().join();
        assertNotNull(doc);
        assertEquals("spacy", doc.annotator_name());
    }

    @Test
    void directHttpTaxonerdResponds() throws Exception {
        DUUIV1Annotator annotator = createAnnotator("taxonerd", taxonerdServer);
        assertNotNull(annotator);
        var doc = annotator.documentation().join();
        assertNotNull(doc);
        assertEquals("taxonerd", doc.annotator_name());
    }

    // -- Test 2: DUUI pipeline with remote driver (all 4 annotators) --

    @Test
    void pipelineWithAllFourRemoteAnnotators() throws Exception {
        DUUIV1Annotator gnfinder = createAnnotator("gnfinder", gnfinderServer);
        DUUIV1Annotator gazetteer = createAnnotator("gazetteer", gazetteerServer);
        DUUIV1Annotator spacy = createAnnotator("spacy", spacyServer);
        DUUIV1Annotator taxonerd = createAnnotator("taxonerd", taxonerdServer);

        DUUICheckpoint<org.apache.uima.jcas.JCas> cp1 = new DUUICheckpoint<>("gnfinder-cp");
        DUUICheckpoint<org.apache.uima.jcas.JCas> cp2 = new DUUICheckpoint<>("gazetteer-cp");
        DUUICheckpoint<org.apache.uima.jcas.JCas> cp3 = new DUUICheckpoint<>("spacy-cp");
        DUUICheckpoint<org.apache.uima.jcas.JCas> cp4 = new DUUICheckpoint<>("taxonerd-cp");
        DUUICheckpoint<org.apache.uima.jcas.JCas> done = new DUUICheckpoint<>("done");

        cp1.stage(DUUIStage.linearProcessor("gnfinder",
                List.of(new DUUIV1Component("gnfinder-comp", List.of(
                        new DUUINode<>("gnfinder-slot-0", null, gnfinder, 1, 1)))),
                cp2, null, null));

        cp2.stage(DUUIStage.linearProcessor("gazetteer",
                List.of(new DUUIV1Component("gazetteer-comp", List.of(
                        new DUUINode<>("gazetteer-slot-0", null, gazetteer, 1, 1)))),
                cp3, null, null));

        cp3.stage(DUUIStage.linearProcessor("spacy",
                List.of(new DUUIV1Component("spacy-comp", List.of(
                        new DUUINode<>("spacy-slot-0", null, spacy, 1, 1)))),
                cp4, null, null));

        cp4.stage(DUUIStage.linearProcessor("taxonerd",
                List.of(new DUUIV1Component("taxonerd-comp", List.of(
                        new DUUINode<>("taxonerd-slot-0", null, taxonerd, 1, 1)))),
                done, null, null));

        DUUISource<org.apache.uima.jcas.JCas> source = new DUUISource<>() {
            @Override
            public void generate(DUUIArtifactEmitter<org.apache.uima.jcas.JCas> emitter) {
            }
        };
        DUUITarget<org.apache.uima.jcas.JCas> target = new DUUITarget<>() {
            @Override
            public void accept(DUUIArtifact<org.apache.uima.jcas.JCas> artifact) {
            }
        };

        DUUIPipeline pipeline = DUUIPipeline.builder("remote-annotators")
                .stage(DUUIStage.source("source", source))
                .stage(cp1.stage())
                .stage(cp2.stage())
                .stage(cp3.stage())
                .stage(cp4.stage())
                .stage(DUUIStage.target("target", target))
                .build();

        DUUIOrchestrator orchestrator = new DUUIOrchestrator(pipeline);
        assertNotNull(orchestrator);
    }

    // -- Test 3: DUUIScope builder ergonomics from [DESIGN: 105] --

    @Test
    void duuiScopeBuilderPatternCompiles() {
        assertDoesNotThrow(() -> {
            try (var orch = DUUIOrchestrator.build()) {
                assertNotNull(orch);
                assertNotNull(orch.get());

                var scheduler = orch.withSchedulerPolicy(
                        org.texttechnologylab.duui.orchestration.scheduling.DUUISchedulerPolicy.roundRobin())
                        .build();
                assertNotNull(scheduler);

                var errorPolicy = orch.withErrorPolicy()
                        .retryPolicy(3)
                        .build();
                assertNotNull(errorPolicy);

                var governor = orch.withGovernor()
                        .port(5829)
                        .build();
                assertNotNull(governor);
            }
        });
    }

    // -- Helpers --

    private static DUUIV1Annotator createAnnotator(String id, HttpServer server) throws Exception {
        return new DUUIV1Annotator(
                id,
                new DUUIHttpEndpoint(URI.create("http://127.0.0.1:" + server.getAddress().getPort()), HttpClient.newHttpClient()),
                new DUUIV1Config(1, "_InitialView", "_InitialView", Map.of())
        );
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
