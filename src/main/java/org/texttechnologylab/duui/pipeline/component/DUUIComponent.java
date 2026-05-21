package org.texttechnologylab.duui.pipeline.component;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.event.DUUIEventContext;
import org.texttechnologylab.duui.event.DUUIEventScope;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutionContext;
import org.texttechnologylab.duui.orchestration.DUUITask;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorker;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DUUIComponent<T> extends DUUIActor implements AutoCloseable {
    private final BlockingQueue<DUUINode<T>> nodes;
    private final int capacity;
    private final AutoCloseable closeAction;

    public DUUIComponent(String id, List<DUUINode<T>> nodes) {
        this(id, nodes, null);
    }

    public DUUIComponent(String id, List<DUUINode<T>> nodes, AutoCloseable closeAction) {
        super(id);
        Objects.requireNonNull(nodes, "nodes");
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("A DUUIComponent requires at least one node.");
        }
        this.nodes = new LinkedBlockingQueue<>(nodes);
        this.capacity = nodes.size();
        this.closeAction = closeAction;
    }

    public static DUUIComponent<org.apache.uima.jcas.JCas> v1(String id, List<DUUIV1Annotator> replicas) {
        Objects.requireNonNull(replicas, "replicas");
        java.util.ArrayList<DUUINode<org.apache.uima.jcas.JCas>> nodes = new java.util.ArrayList<>();
        int slot = 0;
        for (DUUIV1Annotator annotator : replicas) {
            for (int i = 0; i < annotator.config().concurrency(); i++) {
                nodes.add(DUUINode.v1(id + "-slot-" + slot++, annotator));
            }
        }
        return new DUUIComponent<>(id, nodes);
    }

    public static <T> DUUIComponent<T> processor(String id, DUUINodeProcessor<T> processor) {
        return new DUUIComponent<>(id, List.of(new DUUINode<>(id + "-slot-0", processor)));
    }

    public DUUIArtifact<T> process(DUUIArtifact<T> artifact) throws Exception {
        DUUINode<T> node = borrowNode();
        DUUIExecutionContext executionContext = currentExecutionContext();
        DUUIEventContext previous = executionContext == null ? null : executionContext.eventContext();
        try {
            if (executionContext != null) {
                executionContext.eventContext((previous == null ? DUUIEventContext.root(null, null) : previous).toBuilder()
                        .artifactId(artifact.id())
                        .componentId(id())
                        .nodeId(node.id())
                        .annotatorId(node.annotator() == null ? null : node.annotator().id())
                        .build());
            }
            DUUIEventService.current().logger("duui.component").info("Borrowed node " + node.id() + " for component " + id());
            DUUIEventScope scope = DUUIEventService.current().scope("analysis");
            try {
                return node.process(artifact);
            } catch (Exception error) {
                scope.fail(error);
                throw error;
            } finally {
                scope.close();
            }
        } finally {
            if (executionContext != null) {
                executionContext.eventContext(previous);
            }
            returnNode(node);
        }
    }

    private static DUUIExecutionContext currentExecutionContext() {
        try {
            DUUITask<?> task = DUUIWorker.current().currentTask();
            return task == null ? null : task.context();
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    public int capacity() {
        return capacity;
    }

    public DUUINode<T> borrowNode() throws InterruptedException {
        return nodes.take();
    }

    public void returnNode(DUUINode<T> node) {
        nodes.offer(Objects.requireNonNull(node, "node"));
    }

    public int availableNodes() {
        return nodes.size();
    }

    public boolean fork() {
        return false;
    }

    @Override
    public void close() throws Exception {
        if (closeAction != null) {
            closeAction.close();
        }
    }
}
