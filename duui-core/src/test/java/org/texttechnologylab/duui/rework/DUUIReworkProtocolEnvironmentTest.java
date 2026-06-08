package org.texttechnologylab.duui.rework;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.clients.handle.DUUIProxy;
import org.texttechnologylab.duui.clients.hosts.remote.DUUIRemoteEndpoint;
import org.texttechnologylab.duui.clients.hosts.remote.DUUIRemoteEnvironment;
import org.texttechnologylab.duui.clients.hosts.virtualization.DUUIContainer;
import org.texttechnologylab.duui.clients.hosts.virtualization.DUUIContainerImage;
import org.texttechnologylab.duui.clients.hosts.virtualization.DUUIVirtualizationClient;
import org.texttechnologylab.duui.clients.hosts.virtualization.DUUIVirtualizationException;
import org.texttechnologylab.duui.clients.http.DUUIChannel;
import org.texttechnologylab.duui.clients.http.DUUIHttpMethod;
import org.texttechnologylab.duui.clients.http.DUUIHttpResponse;
import org.texttechnologylab.duui.clients.http.DUUIRelay;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.pipeline.component.DUUINode;
import org.texttechnologylab.duui.pipeline.component.DUUIV1Component;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;
import org.texttechnologylab.duui.protocol.v1.DUUIV1TelemetryConfig;
import org.texttechnologylab.duui.timelines.DUUIFlow;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUIReworkProtocolEnvironmentTest {
    @Test
    void remoteEnvironmentResolvesEndpointHandleAndV1ProcessChannel() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String typeSystem = typeSystemXml();
        server.createContext("/v1/documentation", exchange -> respond(exchange, 200, """
                {"annotator_name":"mock","version":"1","description":"mock","implementation_lang":"java","meta":{},"parameters":{}}
                """));
        server.createContext("/v1/typesystem", exchange -> respond(exchange, 200, typeSystem));
        server.createContext("/v1/communication_layer", exchange -> respond(exchange, 200, luaCommunicationLayer()));
        server.createContext("/v1/process", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            respond(exchange, 200, "processed:" + body);
        });
        server.start();
        try {
            DUUIRemoteEnvironment environment = new DUUIRemoteEnvironment();
            DUUIRemoteEndpoint endpoint = environment.endpoint("http://127.0.0.1:" + server.getAddress().getPort());
            DUUIV1Annotator annotator = new DUUIV1Annotator(
                    "mock",
                    endpoint,
                    new DUUIV1Config(1, "_InitialView", "_InitialView", Map.of()));
            DUUIComponent<JCas> component = new DUUIV1Component("mock-component", List.of(
                    new DUUINode<>("mock-component-slot-0", null, annotator)
            ));
            JCas cas = JCasFactory.createJCas();
            cas.setDocumentText("input");

            component.process(DUUIArtifact.of(cas)).join();

            assertEquals("processed:input", cas.getDocumentText());
            assertEquals(endpoint, annotator.endpoint());

            DUUIChannel<String> process = new DUUIChannel<>(endpoint, DUUIHttpMethod.POST, "/v1/process");
            DUUIRelay<String> inputRelay = new DUUIRelay<>();
            DUUIRelay<DUUIHttpResponse> outputRelay = new DUUIRelay<>();
            try (OutputStream output = inputRelay.outputStream()) {
                output.write("input".getBytes(StandardCharsets.UTF_8));
            }
            DUUIHttpResponse response = process.request(inputRelay, outputRelay);

            assertEquals(200, response.statusCode());
            assertEquals("processed:input", new String(outputRelay.inputStream().readAllBytes(), StandardCharsets.UTF_8));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void v1AnnotatorTreatsDocumentationEndpointAsOptional() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String typeSystem = typeSystemXml();
        server.createContext("/v1/documentation", exchange -> respond(exchange, 200, "not-json"));
        server.createContext("/v1/typesystem", exchange -> respond(exchange, 200, typeSystem));
        server.createContext("/v1/communication_layer", exchange -> respond(exchange, 200, luaCommunicationLayer()));
        server.start();
        try {
            DUUIRemoteEnvironment environment = new DUUIRemoteEnvironment();
            DUUIRemoteEndpoint endpoint = environment.endpoint("http://127.0.0.1:" + server.getAddress().getPort());

            DUUIV1Annotator annotator = new DUUIV1Annotator(
                    "optional-docs",
                    endpoint,
                    new DUUIV1Config(1, "_InitialView", "_InitialView", Map.of()));

            assertEquals("optional-docs", annotator.documentation().join().annotator_name());
            assertEquals("unknown", annotator.documentation().join().version());
            assertNotNull(annotator.typesystemDescription().join());
            assertNotNull(annotator.communicationLayer().join());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void v1DriverUsesConfiguredTransportAndTelemetryForV2Config() {
        TestV1Driver driver = new TestV1Driver(
                4,
                "application/x-duui-test",
                new DUUIV1TelemetryConfig(true, 10, null, 250));

        DUUIV1Config config = driver.config;

        assertEquals(4, config.concurrency());
        assertEquals("application/x-duui-test", config.contentType());
        assertTrue(config.telemetry().enabled());
        assertEquals(10, config.telemetry().ttlMinutes());
        assertEquals(250, config.telemetry().sampleIntervalMs());
    }

    @Test
    void dockerStyleEnvironmentHandlesAreProxiesNotComponents() throws Exception {
        FakeVirtualizationClient client = new FakeVirtualizationClient();
        DUUIContainerImage image = client.image("duui/mock:latest");
        DUUIContainer container = image.run(List.of("serve")).join();

        assertTrue(image instanceof DUUIProxy);
        assertTrue(container instanceof DUUIProxy);
        assertEquals("docker-image:/duui/mock:latest", image.address().value());
        assertEquals("docker-container:/container-1", container.address().value());
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static String typeSystemXml() throws Exception {
        StringWriter writer = new StringWriter();
        TypeSystemDescriptionFactory.createTypeSystemDescription().toXML(writer);
        return writer.toString();
    }

    private static String luaCommunicationLayer() {
        return """
                function serialize(view, output, parameters, sourceView)
                  local text = view:getDocumentText()
                  if text ~= nil then
                    output:write(text:getBytes("UTF-8"))
                  end
                end

                function deserialize(view, input)
                  local bytes = input:readAllBytes()
                  local text = luajava.newInstance("java.lang.String", bytes, "UTF-8")
                  view:setDocumentText(text)
                end
                """;
    }

    private static final class FakeVirtualizationClient extends DUUIVirtualizationClient<FakeContainer, FakeImage> {
        private FakeVirtualizationClient() {
            super(org.texttechnologylab.duui.clients.handle.DUUIAddress.parse("docker://local"));
        }

        @Override
        public FakeImage image(String reference) {
            return new FakeImage(reference);
        }

        @Override
        public FakeContainer container(String id) {
            return new FakeContainer(id, image("duui/mock:latest"));
        }

        @Override
        public Stream<FakeContainer> containers() {
            return Stream.of(container("container-1"));
        }
    }

    private static final class TestV1Driver
            extends org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIV1Driver {
        private final DUUIV1Config config;

        private TestV1Driver() {
            this.config = null;
        }

        private TestV1Driver(int concurrency, String contentType, DUUIV1TelemetryConfig telemetry) {
            withV1Transport(true, contentType);
            withV1Telemetry(telemetry);
            this.config = v1Config(concurrency, "_InitialView", "_InitialView", Map.of("k", "v"));
        }

        @Override
        public boolean canAccept(org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent component) {
            return false;
        }

        @Override
        public String instantiate(
                org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent component,
                JCas jc,
                boolean skipVerification,
                java.util.concurrent.atomic.AtomicBoolean shutdown) {
            return null;
        }

        @Override
        public void run(
                String uuid,
                JCas aCas,
                org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance perf,
                org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer composer) {
        }

        @Override
        public boolean destroy(String uuid) {
            return false;
        }
    }

    private static final class FakeImage extends DUUIContainerImage {
        private FakeImage(String reference) {
            super(org.texttechnologylab.duui.clients.handle.DUUIAddress.parse("docker-image:/" + reference), reference, 0, Instant.now());
        }

        @Override
        public DUUIFlow<? extends DUUIContainer> run(List<String> command) {
            return DUUIFlow.dispatch(new FakeContainer("container-1", this));
        }

        @Override
        public DUUIFlow<DUUIContainerImage> pull() {
            return DUUIFlow.dispatch(this);
        }

        @Override
        public DUUIFlow<DUUIContainerImage> push() {
            return DUUIFlow.dispatch(this);
        }

        @Override
        public DUUIFlow<DUUIContainerImage> build(String context) {
            return DUUIFlow.dispatch(this);
        }
    }

    private static final class FakeContainer extends DUUIContainer {
        private FakeContainer(String id, DUUIContainerImage image) {
            super(org.texttechnologylab.duui.clients.handle.DUUIAddress.parse("docker-container:/" + id), id, image, Instant.now());
        }

        @Override
        public DUUIFlow<Boolean> running() {
            return DUUIFlow.dispatch(true);
        }

        @Override
        public DUUIFlow<DUUIContainer> start() throws DUUIVirtualizationException {
            return DUUIFlow.dispatch(this);
        }

        @Override
        public DUUIFlow<DUUIContainer> stop() throws DUUIVirtualizationException {
            return DUUIFlow.dispatch(this);
        }

        @Override
        public DUUIFlow<DUUIContainer> restart() throws DUUIVirtualizationException {
            return DUUIFlow.dispatch(this);
        }

        @Override
        public DUUIFlow<Void> delete() throws DUUIVirtualizationException {
            return DUUIFlow.dispatch(null);
        }
    }
}
