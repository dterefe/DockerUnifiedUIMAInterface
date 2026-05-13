package org.texttechnologylab.duui.rework;

import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.pipeline.DUUIAdapter;
import org.texttechnologylab.duui.pipeline.DUUIFork;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.pipeline.DUUILambda;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIAdapterScope;
import org.texttechnologylab.duui.runtime.DUUIFlowScope;
import org.texttechnologylab.duui.runtime.DUUIForkScope;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DUUIScopedAssemblyTest {
    private static final DUUIArtifactType<UCEImport> UCE_IMPORT = DUUIArtifactType.of("uce/import");
    private static final DUUIArtifactType<UCECorpus> UCE_CORPUS = DUUIArtifactType.of("uce/corpus");
    private static final DUUIArtifactType<UCEDocument> UCE_DOCUMENT = DUUIArtifactType.of("uce/document");

    @Test
    void scopedPipelineRunsGeneratorAdapterForkLambdaAndJoin() {
        List<UCEDocument> persisted = new ArrayList<>();
        List<UCECorpus> finalized = new ArrayList<>();

        DUUIOrchestrationResult result;
        try (var duui = DUUI.system("scoped-test")) {
            try (DUUIPipelineScope pipeline = duui.pipeline("routing")) {
                try (DUUIGeneratorScope<UCEImport> imports = ImportReader.builder().root("input").open(pipeline)) {
                    try (DUUIAdapterScope<UCEImport, UCECorpus> corpora = ImportToCorpus.builder().open(imports)) {
                        try (DUUIForkScope<UCECorpus, UCEDocument> documents = DocumentReader.builder().open(corpora)) {
                            try (DUUIStageScope<UCEDocument> stage = documents.linear("document-procedures")) {
                                stage.lambda(LoadDocument.builder().build());
                                stage.lambda(PersistDocument.builder().target(persisted).build());
                            }
                        }
                        try (DUUIStageScope<UCECorpus> stage = corpora.linear("finalize-corpus")) {
                            stage.lambda(FinalizeCorpus.builder().target(finalized).build());
                        }
                    }
                }
            }

            result = duui.run("routing");
        }

        assertEquals(0, result.unroutableArtifacts().size());
        assertEquals(2, persisted.size());
        assertEquals(1, finalized.size());
    }

    private record UCEImport(String root) {}
    private record UCECorpus(String id) {}
    private record UCEDocument(String id, boolean loaded) {}

    private static final class ImportReader implements DUUIGenerator<UCEImport> {
        private final String root;

        private ImportReader(String root) {
            this.root = root;
        }

        static Builder builder() {
            return new Builder();
        }

        @Override
        public DUUIArtifactType<UCEImport> outputType() {
            return UCE_IMPORT;
        }

        @Override
        public void generate(DUUIArtifactEmitter<UCEImport> emitter) throws Exception {
            emitter.emit(DUUIArtifact.of(new UCEImport(root), UCE_IMPORT));
        }

        static final class Builder {
            private String root;

            Builder root(String root) {
                this.root = root;
                return this;
            }

            DUUIGeneratorScope<UCEImport> open(DUUIPipelineScope pipeline) {
                return new ImportReader(root).open(pipeline);
            }
        }
    }

    private static final class ImportToCorpus implements DUUIAdapter<UCEImport, UCECorpus> {
        static Builder builder() {
            return new Builder();
        }

        @Override
        public DUUIArtifactType<UCEImport> inputType() {
            return UCE_IMPORT;
        }

        @Override
        public DUUIArtifactType<UCECorpus> outputType() {
            return UCE_CORPUS;
        }

        @Override
        public DUUIArtifact<UCECorpus> adapt(DUUIArtifact<UCEImport> artifact) throws Exception {
            return successor(artifact, new UCECorpus(artifact.payload().root()));
        }

        static final class Builder {
            DUUIAdapterScope<UCEImport, UCECorpus> open(DUUIFlowScope<UCEImport> parent) {
                return parent.pipeline().adapter(parent, new ImportToCorpus());
            }
        }
    }

    private static final class DocumentReader implements DUUIFork<UCECorpus, UCEDocument> {
        static Builder builder() {
            return new Builder();
        }

        @Override
        public DUUIArtifactType<UCECorpus> inputType() {
            return UCE_CORPUS;
        }

        @Override
        public DUUIArtifactType<UCEDocument> outputType() {
            return UCE_DOCUMENT;
        }

        @Override
        public void fork(DUUIArtifact<UCECorpus> artifact, DUUIArtifactEmitter<UCEDocument> emitter) throws Exception {
            emitter.emit(child(artifact, new UCEDocument("doc-1", false)));
            emitter.emit(child(artifact, new UCEDocument("doc-2", false)));
        }

        static final class Builder {
            DUUIForkScope<UCECorpus, UCEDocument> open(DUUIFlowScope<UCECorpus> parent) {
                return parent.pipeline().fork(parent, new DocumentReader());
            }
        }
    }

    private static final class LoadDocument implements DUUILambda<UCEDocument> {
        static Builder builder() {
            return new Builder();
        }

        @Override
        public DUUIArtifactType<UCEDocument> inputType() {
            return UCE_DOCUMENT;
        }

        @Override
        public DUUIArtifact<UCEDocument> process(DUUIArtifact<UCEDocument> artifact) throws Exception {
            return artifact.successorArtifact(new UCEDocument(artifact.payload().id(), true), UCE_DOCUMENT);
        }

        static final class Builder {
            LoadDocument build() {
                return new LoadDocument();
            }
        }
    }

    private static final class PersistDocument implements DUUILambda<UCEDocument> {
        private final List<UCEDocument> target;

        private PersistDocument(List<UCEDocument> target) {
            this.target = target;
        }

        static Builder builder() {
            return new Builder();
        }

        @Override
        public DUUIArtifactType<UCEDocument> inputType() {
            return UCE_DOCUMENT;
        }

        @Override
        public DUUIArtifact<UCEDocument> process(DUUIArtifact<UCEDocument> artifact) throws Exception {
            target.add(artifact.payload());
            return artifact;
        }

        static final class Builder {
            private List<UCEDocument> target;

            Builder target(List<UCEDocument> target) {
                this.target = target;
                return this;
            }

            PersistDocument build() {
                return new PersistDocument(target);
            }
        }
    }

    private static final class FinalizeCorpus implements DUUILambda<UCECorpus> {
        private final List<UCECorpus> target;

        private FinalizeCorpus(List<UCECorpus> target) {
            this.target = target;
        }

        static Builder builder() {
            return new Builder();
        }

        @Override
        public DUUIArtifactType<UCECorpus> inputType() {
            return UCE_CORPUS;
        }

        @Override
        public DUUIArtifact<UCECorpus> process(DUUIArtifact<UCECorpus> artifact) throws Exception {
            target.add(artifact.payload());
            return artifact;
        }

        static final class Builder {
            private List<UCECorpus> target;

            Builder target(List<UCECorpus> target) {
                this.target = target;
                return this;
            }

            FinalizeCorpus build() {
                return new FinalizeCorpus(target);
            }
        }
    }
}
