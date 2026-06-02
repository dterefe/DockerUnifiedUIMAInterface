package org.texttechnologylab.duui.pipeline;

import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.pipeline.component.DUUIAnnotator;

import java.util.Objects;

public final class DUUIAnalysisEngine implements DUUIAnnotator<JCas>, DUUIProcessor<JCas> {
    private final GID gid;
    private final DUUITraits traits;
    private final String id;
    private final AnalysisEngine engine;
    private final String sourceView;

    private DUUIAnalysisEngine(String id, AnalysisEngine engine, String sourceView) {
        this.gid = GID.create(DUUIAnalysisEngine.class);
        this.traits = DUUITraits.empty();
        this.id = Objects.requireNonNull(id, "id");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.sourceView = sourceView == null ? "_InitialView" : sourceView;
    }

    public static Builder builder(String id) {
        return new Builder(id);
    }

    @Override
    public GID gid() {
        return gid;
    }

    @Override
    public DUUITraits traits() {
        return traits;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public DUUIArtifact<JCas> process(DUUIArtifact<JCas> artifact) throws Exception {
        JCas view = artifact.payload().getView(sourceView);
        engine.process(view);
        return artifact;
    }

    public void shutdown() {
        engine.destroy();
    }

    public static final class Builder {
        private final String id;
        private AnalysisEngineDescription description;
        private AnalysisEngine engine;
        private String sourceView = "_InitialView";

        private Builder(String id) {
            this.id = Objects.requireNonNull(id, "id");
        }

        public Builder analysisEngine(AnalysisEngineDescription description) {
            this.description = description;
            return this;
        }

        public Builder analysisEngine(AnalysisEngine engine) {
            this.engine = engine;
            return this;
        }

        public Builder sourceView(String sourceView) {
            this.sourceView = sourceView == null ? "_InitialView" : sourceView;
            return this;
        }

        public DUUIAnalysisEngine build() throws Exception {
            AnalysisEngine resolved = engine == null
                    ? AnalysisEngineFactory.createEngine(Objects.requireNonNull(description, "description"))
                    : engine;
            return new DUUIAnalysisEngine(id, resolved, sourceView);
        }
    }
}
