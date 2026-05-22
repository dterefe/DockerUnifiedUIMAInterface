package org.texttechnologylab.duui.rework;

import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.communication.DUUICommunicationLayer;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrator;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.pipeline.DUUIAdapter;
import org.texttechnologylab.duui.pipeline.DUUIExecutionMode;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.pipeline.component.DUUINode;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DUUIReworkPipelineTest {
    @Test
    void componentCreatesSlotsFromReplicasAndPreservesAnnotatorIdentity() throws Exception {
        DUUIV1Annotator first = annotator("first", 2);
        DUUIV1Annotator second = annotator("second", 1);
        DUUIComponent<JCas> component = DUUIComponent.v1("component", List.of(first, second));

        assertEquals(3, component.capacity());

        DUUINode<JCas> firstSlot = component.borrowNode();
        DUUINode<JCas> secondSlot = component.borrowNode();
        DUUINode<JCas> thirdSlot = component.borrowNode();

        assertSame(first, firstSlot.annotator());
        assertSame(first, secondSlot.annotator());
        assertSame(second, thirdSlot.annotator());

        component.returnNode(firstSlot);
        component.returnNode(secondSlot);
        component.returnNode(thirdSlot);

        assertEquals(3, component.availableNodes());
    }

    @Test
    void orchestratorDrainsEmittedArtifactsIntoDownstreamCheckpoints() {
        DUUIAdapter<String, Integer> emitLength = new DUUIAdapter<>() {
            @Override
            public DUUIArtifact<Integer> adapt(DUUIArtifact<String> artifact) throws Exception {
                return DUUIArtifact.of(artifact.payload().length());
            }
        };
        DUUIComponent<Integer> increment = DUUIComponent.processor(
                "increment",
                artifact -> DUUIArtifact.of(artifact.payload() + 1));

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
        DUUIComponent<String> identity = DUUIComponent.processor("identity", artifact -> artifact);

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
                DUUIComponent.processor("uppercase", artifact -> DUUIArtifact.of(artifact.payload().toUpperCase()))
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

    private static DUUIV1Annotator annotator(String id, int concurrency) throws Exception {
        return new DUUIV1Annotator(
                id,
                new DUUIHttpEndpoint(URI.create("http://localhost/" + id), HttpClient.newHttpClient()),
                new DUUIV1Config(concurrency, "_InitialView", "_InitialView", Map.of()),
                new DUUIV1Annotator.Documentation(id, "test", "test", "java", Map.of(), Map.of()),
                TypeSystemDescriptionFactory.createTypeSystemDescription(),
                new NoopCommunicationLayer(),
                ignored -> { }
        );
    }

    private static final class NoopCommunicationLayer implements DUUICommunicationLayer {
        @Override
        public void serialize(JCas sourceCas, OutputStream output, Map<String, String> parameters, String sourceView) throws CASException {
        }

        @Override
        public void deserialize(JCas targetCas, InputStream input, String targetView) throws CASException {
        }

        @Override
        public DUUICommunicationLayer copy() {
            return new NoopCommunicationLayer();
        }
    }
}
