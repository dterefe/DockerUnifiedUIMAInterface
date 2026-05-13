package org.texttechnologylab.duui.runtime;

import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.pipeline.DUUIComponent;
import org.texttechnologylab.duui.pipeline.DUUINode;

import java.util.ArrayList;
import java.util.List;

public final class DUUIUimaComponentBuilder implements DUUIStageContribution {
    private final DUUIStageScope<?> stage;
    private final String id;
    private AnalysisEngineDescription description;
    private String sourceView = "_InitialView";
    private String targetView = "_InitialView";
    private int scale = 1;

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
            for (int i = 0; i < scale; i++) {
                AnalysisEngine engine = AnalysisEngineFactory.createEngine(description);
                nodes.add(new DUUINode<>(id + "-slot-" + i, artifact -> {
                    JCas cas = artifact.payload();
                    JCas view = cas.getView(sourceView);
                    engine.process(view);
                    return artifact;
                }));
            }
            stage.jcasComponent(new DUUIComponent<>(id, nodes));
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
