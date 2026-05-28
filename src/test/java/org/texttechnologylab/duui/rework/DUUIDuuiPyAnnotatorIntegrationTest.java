package org.texttechnologylab.duui.rework;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.runtime.DUUI;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;
import org.texttechnologylab.duui.runtime.DUUIStageScope;
import org.texttechnologylab.duui.runtime.DUUISystemScope;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DUUIDuuiPyAnnotatorIntegrationTest {
    @Test
    void migratedV1AnnotatorProcessesAgainstRealDuuiPyAnnotator() throws Exception {
        URI base = URI.create(System.getProperty("duui.py.endpoint", "http://127.0.0.1:19714"));
        HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(base.resolve("/v1/documentation")).timeout(Duration.ofSeconds(2)).GET().build(),
                HttpResponse.BodyHandlers.discarding());

        JCas cas = JCasFactory.createJCas();
        cas.setDocumentText("duui py works");

        DUUIOrchestrationResult result;
        try (DUUISystemScope system = DUUI.system("duui-py-uppercase-system")) {
            try (DUUIPipelineScope pipeline = system.pipeline("duui-py-uppercase-pipeline")) {
                try (DUUIGeneratorScope<JCas> documents = new SingleJCasSource(cas).open(pipeline)) {
                    try (DUUIStageScope<JCas> remote = documents.linear("remote-uppercase")) {
                        remote.v1("duui-py-uppercase")
                                .remote()
                                .endpoint(base.toString())
                                .sourceView("_InitialView")
                                .targetView("duui_py_result")
                                .telemetry()
                                .parameters(Map.of());
                    }
                }
            }
            result = system.run("duui-py-uppercase-pipeline");
        }

        assertFalse(result.hasFailures());
        assertEquals("DUUI PY WORKS", cas.getView("duui_py_result").getDocumentText());
    }

    private record SingleJCasSource(JCas cas) implements DUUIGenerator<JCas> {
        @Override
        public void generate(Emitter<JCas> emitter) {
            emitter.emit(DUUIArtifact.of(cas));
        }
    }
}
