package org.texttechnologylab.duui.rework;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.clients.hosts.remote.DUUIRemoteEndpoint;
import org.texttechnologylab.duui.clients.hosts.remote.DUUIRemoteEnvironment;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DUUIDuuiPyAnnotatorIntegrationTest {
    @Test
    void migratedV1AnnotatorProcessesAgainstRealDuuiPyAnnotator() throws Exception {
        URI base = URI.create(System.getProperty("duui.py.endpoint", "http://127.0.0.1:19714"));
        HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(base.resolve("/v1/documentation")).timeout(Duration.ofSeconds(2)).GET().build(),
                HttpResponse.BodyHandlers.discarding());

        DUUIRemoteEndpoint endpoint = new DUUIRemoteEnvironment().endpoint(base.toString());
        DUUIV1Annotator annotator = new DUUIV1Annotator(
                "duui-py-uppercase",
                endpoint,
                new DUUIV1Config(1, "_InitialView", "duui_py_result", Map.of()));

        JCas cas = JCasFactory.createJCas();
        cas.setDocumentText("duui py works");

        annotator.process(DUUIArtifact.of(cas, JCas.class));

        assertEquals("duui-py-uppercase", annotator.documentation().annotator_name());
        assertEquals("DUUI PY WORKS", cas.getView("duui_py_result").getDocumentText());
    }
}
