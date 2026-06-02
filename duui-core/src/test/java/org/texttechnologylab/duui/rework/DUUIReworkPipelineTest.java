package org.texttechnologylab.duui.rework;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrator;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.pipeline.DUUIAdapter;
import org.texttechnologylab.duui.pipeline.DUUIExecutionMode;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.pipeline.component.DUUINode;
import org.texttechnologylab.duui.pipeline.component.DUUIV1Component;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;
import org.texttechnologylab.duui.timelines.DUUIFlow;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DUUIReworkPipelineTest {
    @Test
    void componentCreatesSlotsFromReplicasAndPreservesAnnotatorIdentity() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        StringWriter writer = new StringWriter();
        TypeSystemDescriptionFactory.createTypeSystemDescription().toXML(writer);
        String typeSystem = writer.toString();
        server.createContext("/v1/documentation", exchange -> respond(exchange, 200, """
                {"annotator_name":"mock","version":"1","description":"mock","implementation_lang":"java","meta":{},"parameters":{}}
                """));
        server.createContext("/v1/typesystem", exchange -> respond(exchange, 200, typeSystem));
        server.createContext("/v1/communication_layer", exchange -> respond(exchange, 200, """
                function serialize(view, output, parameters, sourceView)
                end

                function deserialize(view, input)
                end
                """));
        server.start();
        try {
            DUUIV1Annotator first = annotator("first", 2, server);
            DUUIV1Annotator second = annotator("second", 1, server);
            DUUIComponent<org.apache.uima.jcas.JCas> component = new DUUIV1Component("component", List.of(
                    new DUUINode<>("component-slot-0", null, first),
                    new DUUINode<>("component-slot-1", null, first),
                    new DUUINode<>("component-slot-2", null, second)
            ));

            assertEquals(3, component.capacity());

            DUUINode<org.apache.uima.jcas.JCas> firstSlot = component.borrowNode();
            DUUINode<org.apache.uima.jcas.JCas> secondSlot = component.borrowNode();
            DUUINode<org.apache.uima.jcas.JCas> thirdSlot = component.borrowNode();

            assertSame(first, firstSlot.annotator());
            assertSame(first, secondSlot.annotator());
            assertSame(second, thirdSlot.annotator());

            component.returnNode(firstSlot);
            component.returnNode(secondSlot);
            component.returnNode(thirdSlot);

            assertEquals(3, component.availableNodes());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void orchestratorDrainsEmittedArtifactsIntoDownstreamCheckpoints() {
        DUUIAdapter<String, Integer> emitLength = new DUUIAdapter<>() {
            @Override
            public DUUIArtifact<Integer> adapt(DUUIArtifact<String> artifact) throws Exception {
                return DUUIArtifact.of(artifact.payload().length());
            }
        };
        DUUIComponent<Integer> increment = new DUUIComponent<>("increment", List.of(new DUUINode<>(
                "increment-slot-0",
                artifact -> DUUIArtifact.of(artifact.payload() + 1)))) {
            @Override
            public DUUIFlow<DUUIArtifact<Integer>> process(DUUIArtifact<Integer> artifact) {
                DUUINode<Integer> node;
                try {
                    node = borrowNode();
                } catch (InterruptedException error) {
                    return DUUIFlow.cancel(error);
                }
                try {
                    return DUUIFlow.dispatch(node.processor().process(artifact));
                } catch (Exception error) {
                    return DUUIFlow.fail(error);
                } finally {
                    returnNode(node);
                }
            }
        };

        DUUICheckpoint<String> strings = new DUUICheckpoint<>("strings");
        DUUICheckpoint<Integer> integers = new DUUICheckpoint<>("integers");
        DUUICheckpoint<Integer> done = new DUUICheckpoint<>("done");
        strings.stage(DUUIStage.adapter("emitLength", emitLength, integers));
        integers.stage(DUUIStage.processor("increment", DUUIExecutionMode.LINEAR, List.of(increment), done, null, null));

        DUUIPipeline pipeline = DUUIPipeline.builder("routing")
                .checkpoint(strings)
                .checkpoint(integers)
                .checkpoint(done)
                .build();

        DUUIOrchestrationResult result = new DUUIOrchestrator(pipeline).run(DUUIArtifact.of("abcd"));

        assertEquals(3, result.results().size());
        assertEquals(0, result.unroutableArtifacts().size());
        assertEquals(5, result.results().get(2).artifact().payload());
    }

    @Test
    void checkpointRejectsMultipleStages() {
        DUUICheckpoint<String> checkpoint = new DUUICheckpoint<>("single-stage");
        DUUICheckpoint<String> output = new DUUICheckpoint<>("output");
        DUUIComponent<String> identity = new DUUIComponent<>("identity", List.of(new DUUINode<>("identity-slot-0", artifact -> artifact))) {
            @Override
            public DUUIFlow<DUUIArtifact<String>> process(DUUIArtifact<String> artifact) {
                DUUINode<String> node;
                try {
                    node = borrowNode();
                } catch (InterruptedException error) {
                    return DUUIFlow.cancel(error);
                }
                try {
                    return DUUIFlow.dispatch(node.processor().process(artifact));
                } catch (Exception error) {
                    return DUUIFlow.fail(error);
                } finally {
                    returnNode(node);
                }
            }
        };

        checkpoint.stage(DUUIStage.processor("first", DUUIExecutionMode.LINEAR, List.of(identity), output, null, null));

        assertThrows(IllegalStateException.class, () ->
                checkpoint.stage(DUUIStage.processor("second", DUUIExecutionMode.LINEAR, List.of(identity), output, null, null)));
    }

    @Test
    void splitJoinReleasesAggregateAfterAllRelatedArtifactsComplete() {
        DUUICheckpoint<String> parent = new DUUICheckpoint<>("parent");
        DUUICheckpoint<String> parts = new DUUICheckpoint<>("parts");
        DUUICheckpoint<String> join = new DUUICheckpoint<>("join");
        DUUICheckpoint<String> done = new DUUICheckpoint<>("done");

        parent.stage(DUUIStage.split("split", (artifact, emitter) -> {
            for (String part : artifact.payload().split("")) {
                emitter.emit(DUUIArtifact.of(part));
            }
        }, parts, new DUUICheckpoint<>("after-split")));
        parts.stage(DUUIStage.processor("uppercase", DUUIExecutionMode.LINEAR, List.of(
                new DUUIComponent<>("uppercase", List.of(new DUUINode<>(
                        "uppercase-slot-0",
                        artifact -> DUUIArtifact.of(artifact.payload().toUpperCase())))) {
                    @Override
                    public DUUIFlow<DUUIArtifact<String>> process(DUUIArtifact<String> artifact) {
                        DUUINode<String> node;
                        try {
                            node = borrowNode();
                        } catch (InterruptedException error) {
                            return DUUIFlow.cancel(error);
                        }
                        try {
                            return DUUIFlow.dispatch(node.processor().process(artifact));
                        } catch (Exception error) {
                            return DUUIFlow.fail(error);
                        } finally {
                            returnNode(node);
                        }
                    }
                }
        ), join, null, null));
        join.stage(DUUIStage.join("join", artifacts -> DUUIArtifact.of(artifacts.stream()
                .map(DUUIArtifact::payload)
                .sorted(Comparator.naturalOrder())
                .reduce("", String::concat)), done));

        DUUIPipeline pipeline = DUUIPipeline.builder("split-join")
                .checkpoint(parent)
                .checkpoint(parts)
                .checkpoint(join)
                .checkpoint(done)
                .build();

        DUUIOrchestrationResult result = new DUUIOrchestrator(pipeline).run(DUUIArtifact.of("cab"));

        assertEquals(0, result.unroutableArtifacts().size());
        assertEquals("ABC", result.results().get(result.results().size() - 1).artifact().payload());
    }

    private static DUUIV1Annotator annotator(String id, int concurrency, HttpServer server) throws Exception {
        return new DUUIV1Annotator(
                id,
                new DUUIHttpEndpoint(URI.create("http://127.0.0.1:" + server.getAddress().getPort()), HttpClient.newHttpClient()),
                new DUUIV1Config(concurrency, "_InitialView", "_InitialView", Map.of())
        );
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
