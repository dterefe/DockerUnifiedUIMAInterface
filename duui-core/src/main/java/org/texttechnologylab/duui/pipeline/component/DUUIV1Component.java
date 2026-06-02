package org.texttechnologylab.duui.pipeline.component;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.timelines.DUUIFlow;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DUUIV1Component extends DUUIComponent<JCas> {
    public DUUIV1Component(String id, List<DUUINode<JCas>> nodes) {
        super(id, nodes);
    }

    public DUUIV1Component(String id, List<DUUINode<JCas>> nodes, AutoCloseable closeAction) {
        super(id, nodes, closeAction);
    }

    @Override
    public DUUIFlow<DUUIArtifact<JCas>> process(DUUIArtifact<JCas> artifact) {
        Objects.requireNonNull(artifact, "artifact");
        DUUINode<JCas> node;
        try {
            node = borrowNode();
        } catch (InterruptedException error) {
            return DUUIFlow.cancel(error);
        }

        DUUIV1Annotator annotator = v1Annotator(node);
        DUUIV1Annotator.DUUIPipe pipe = null;
        try {
            pipe = annotator.borrowPipe();
            DUUIV1Annotator.DUUIPipe borrowedPipe = pipe;
            DUUIFlow<Void> serialize = annotator.serialize(artifact.payload(), borrowedPipe.inputRelay());
            DUUIFlow<DUUIArtifact<JCas>> analyse = annotator.analyse(artifact, borrowedPipe.inputRelay(), borrowedPipe.outputRelay());
            DUUIFlow<Void> deserialize = annotator.deserialize(artifact.payload(), borrowedPipe.outputRelay());

            AtomicBoolean released = new AtomicBoolean(false);
            deserialize.onCompleted(ignored -> release(released, node, borrowedPipe))
                    .onFailed(error -> release(released, node, borrowedPipe))
                    .onCancelled(error -> release(released, node, borrowedPipe));
            releaseOnUnsuccessful(serialize, released, node, borrowedPipe);
            releaseOnUnsuccessful(analyse, released, node, borrowedPipe);
            return analyse;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            release(node, pipe);
            return DUUIFlow.cancel(error);
        } catch (Exception error) {
            if (pipe != null) {
                annotator.cancelPipe(pipe, error);
            }
            release(node, pipe);
            return DUUIFlow.fail(error);
        }
    }

    private void releaseOnUnsuccessful(DUUIFlow<?> flow, AtomicBoolean released, DUUINode<JCas> node, DUUIV1Annotator.DUUIPipe pipe) {
        flow.onFailed(error -> release(released, node, pipe))
                .onCancelled(error -> release(released, node, pipe));
    }

    private void release(AtomicBoolean released, DUUINode<JCas> node, DUUIV1Annotator.DUUIPipe pipe) {
        if (released.compareAndSet(false, true)) {
            release(node, pipe);
        }
    }

    private void release(DUUINode<JCas> node, DUUIV1Annotator.DUUIPipe pipe) {
        try {
            if (pipe != null) {
                v1Annotator(node).returnPipe(pipe);
            }
        } catch (Exception error) {
            v1Annotator(node).cancelPipe(pipe, error);
        } finally {
            returnNode(node);
        }
    }

    private static DUUIV1Annotator v1Annotator(DUUINode<JCas> node) {
        if (node.annotator() instanceof DUUIV1Annotator annotator) {
            return annotator;
        }
        throw new IllegalStateException("DUUIV1Component requires DUUIV1Annotator nodes.");
    }
}
