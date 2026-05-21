package org.texttechnologylab.duui.rework;

import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.communication.DUUICommunicationLayer;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrator;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.pipeline.component.DUUIComponents;
import org.texttechnologylab.duui.pipeline.DUUIAdapter;
import org.texttechnologylab.duui.pipeline.component.DUUINode;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

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
        DUUIArtifactType<String> stringType = DUUIArtifactType.javaType(String.class);
        DUUIArtifactType<Integer> integerType = DUUIArtifactType.javaType(Integer.class);
        DUUIComponent<String> emitLength = DUUIComponents.adapter(new DUUIAdapter<String, Integer>() {
            @Override
            public DUUIArtifactType<String> inputType() {
                return stringType;
            }

            @Override
            public DUUIArtifactType<Integer> outputType() {
                return integerType;
            }

            @Override
            public DUUIArtifact<Integer> adapt(DUUIArtifact<String> artifact) throws Exception {
                return artifact.childArtifact(artifact.payload().length(), integerType);
            }
        });
        DUUIComponent<Integer> increment = DUUIComponent.processor(
                "increment",
                artifact -> artifact.successorArtifact(artifact.payload() + 1, Integer.class));

        DUUIPipeline pipeline = DUUIPipeline.builder("routing")
                .checkpoint(DUUICheckpoint.<String>builder("strings", String.class).component("emitLength", emitLength).build())
                .checkpoint(DUUICheckpoint.<Integer>builder("integers", Integer.class).component("increment", increment).build())
                .build();

        DUUIOrchestrationResult result = new DUUIOrchestrator(pipeline).run(DUUIArtifact.of("abcd", String.class));

        assertEquals(2, result.results().size());
        assertEquals(0, result.unroutableArtifacts().size());
        assertEquals(Integer.class, result.results().get(1).artifact().payloadType());
        assertEquals(5, result.results().get(1).artifact().payload());
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
