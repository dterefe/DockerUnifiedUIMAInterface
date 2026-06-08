package org.texttechnologylab.duui.rework;

import org.apache.uima.cas.SerialFormat;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasIOUtils;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPodmanDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIInMemoryEventSink;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.pipeline.io.DUUIXmiCollectionReader;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;
import org.texttechnologylab.duui.runtime.DUUISystemScope;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DUUIUimaAnnotatorMatrixTest {

    private static final String IMAGE = "localhost/duui-gnfinder-v2:latest";
    private static final String OUTPUT_TYPE = "org.texttechnologylab.annotation.biofid.gnfinder.Taxon";

    private static final String TEXT =
            "Nach Schluß des Congresses ist eine längere Excursion vorgesehen, auf welcher die Inseln an der Küste von Pembrokshire besucht werden. "
            + "Dieser Ausflug dürfte besonders interessant werden, weil sich hier große Brutkolonien von Puffinus puffinus und verschiedener Alcidae befinden. "
            + "Auch Thalassidroma pelagica dürfte hier angetroffen werden. "
            + "Bei günstigem Wetter ist ferner der Besuch einer Brutkolonie von Sula bassana vorgesehen.";

    @Test
    void orchestrator() throws Exception {
        TypeSystemDescription ts = TypeSystemDescriptionFactory.createTypeSystemDescription();
        Path input = xmiDir("orchestrator", ts);
        DUUIInMemoryEventSink sink = new DUUIInMemoryEventSink();

        DUUIOrchestrationResult result;
        try (DUUISystemScope system = DUUI.system("uima-orch").events(new DUUIEventService(List.of(sink)))) {
            try (DUUIPipelineScope pipeline = system.pipeline("gnfinder-v2")) {
                try (DUUIGeneratorScope<JCas> source = DUUIXmiCollectionReader.builder()
                        .typeSystem(ts).source(input).open(pipeline)) {
                    try (DUUIStageScope<JCas> stage = source.linear("stage")) {
                        stage.v1("gnfinder")
                                .podman().image(IMAGE)
                                .sourceView("_InitialView").targetView("_InitialView")
                                .telemetrySink(sink)
                                .timeoutSeconds(120L).scale(1).concurrency(1);
                    }
                }
            }
            result = system.run("gnfinder-v2");
        }
        assertFalse(result.hasFailures(), "orchestrator has failures");

        for (DUUIExecutionResult<?> exec : result.results()) {
            if (exec.artifact().payload() instanceof JCas cas) {
                var t = cas.getCas().getTypeSystem().getType(OUTPUT_TYPE);
                int count = t != null ? cas.getCas().getAnnotationIndex(t).size() : 0;
                assertTrue(count > 0, "orchestrator: no annotations");
                return;
            }
        }
        fail("orchestrator: no JCas artifacts");
    }

    @Test
    void composer() throws Exception {
        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText(TEXT);
        jCas.setDocumentLanguage("de");

        DUUIComposer composer = new DUUIComposer()
                .withLuaContext(new DUUILuaContext().withJsonLibrary())
                .withSkipVerification(true);
        composer.addDriver(new DUUIPodmanDriver());
        composer.add(new DUUIPodmanDriver.Component(IMAGE).build());
        composer.run(jCas);
        composer.shutdown();

        var t = jCas.getCas().getTypeSystem().getType(OUTPUT_TYPE);
        int count = t != null ? jCas.getCas().getAnnotationIndex(t).size() : 0;
        assertTrue(count > 0, "composer: no annotations");
    }

    private Path xmiDir(String id, TypeSystemDescription ts) throws Exception {
        Path dir = Path.of(System.getProperty("java.io.tmpdir"), "duui-uima", id + "-input");
        Files.createDirectories(dir);
        JCas cas = JCasFactory.createJCas(ts);
        cas.setDocumentText(TEXT);
        cas.setDocumentLanguage("de");
        Path file = dir.resolve("doc.xmi");
        try (OutputStream os = Files.newOutputStream(file)) {
            CasIOUtils.save(cas.getCas(), os, SerialFormat.XMI_1_1);
        }
        return dir;
    }
}
