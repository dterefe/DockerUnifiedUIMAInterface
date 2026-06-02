package org.texttechnologylab.duui.runtime;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.pipeline.DUUIAnalysisEngine;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.pipeline.component.DUUINode;
import org.texttechnologylab.duui.timelines.DUUIFlow;

import java.util.ArrayList;
import java.util.List;

public final class DUUIUimaComponentBuilder implements DUUIStageContribution {
    private final DUUIStageScope<?> stage;
    private final String id;
    private AnalysisEngineDescription description;
    private String sourceView = "_InitialView";
    private String targetView = "_InitialView";
    private int scale = 1;

    // UIMA AnalysisEngines are not thread-safe; concurrent access is not allowed.
    // Concurrency is hardcoded to 1 — each replica gets exactly ONE node.
    // There is no setter for concurrency and no way to override this.

    DUUIUimaComponentBuilder(DUUIStageScope<?> stage, String id) {
        this.stage = stage;
        this.id = id;
    }

    public DUUIUimaComponentBuilder analysisEngine(AnalysisEngineDescription description) {
        this.description = description;
        return this;
    }

    @Override
    public void contribute() {
        if (description == null) {
            throw new IllegalStateException("DUUI UIMA component requires an analysis engine description: " + id);
        }
        if (!sourceView.equals(targetView)) {
            throw new IllegalStateException("Generic DUUI UIMA components process one JCas view; use a DUUILambda adapter for view transfer before " + id);
        }
        try {
            List<DUUINode<JCas>> nodes = new ArrayList<>();
            List<DUUIAnalysisEngine> engines = new ArrayList<>();
            for (int i = 0; i < scale; i++) {
                DUUIAnalysisEngine engine = DUUIAnalysisEngine.builder(id + "-slot-" + i)
                        .analysisEngine(description)
                        .sourceView(sourceView)
                        .build();
                engines.add(engine);
                // Concurrency is always 1: UIMA AnalysisEngines are not thread-safe.
                // Each scale unit produces exactly one node.
                nodes.add(new DUUINode<>(engine.id(), engine, engine));
            }
            stage.jcasComponent(new DUUIComponent<>(id, nodes, () -> {
                for (DUUIAnalysisEngine engine : engines) {
                    engine.shutdown();
                }
            }) {
                @Override
                public DUUIFlow<DUUIArtifact<JCas>> process(DUUIArtifact<JCas> artifact) {
                    DUUINode<JCas> node;
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
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build DUUI UIMA component: " + id, e);
        }
    }

    public DUUIUimaComponentBuilder sourceView(String sourceView) {
        this.sourceView = sourceView == null ? "_InitialView" : sourceView;
        return this;
    }

    public DUUIUimaComponentBuilder targetView(String targetView) {
        this.targetView = targetView == null ? "_InitialView" : targetView;
        return this;
    }

    public DUUIUimaComponentBuilder scale(int scale) {
        this.scale = Math.max(1, scale);
        return this;
    }

    public String sourceView() { return sourceView; }
    public String targetView() { return targetView; }
    public int scale() { return scale; }
}
