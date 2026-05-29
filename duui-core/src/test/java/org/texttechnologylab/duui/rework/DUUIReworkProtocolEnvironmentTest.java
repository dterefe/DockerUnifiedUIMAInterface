package org.texttechnologylab.duui.rework;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.cas.CASException;
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
import org.texttechnologylab.duui.communication.DUUICommunicationLayer;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
            SimpleCommunicationLayer communicationLayer = new SimpleCommunicationLayer();
            DUUIV1Annotator annotator = new DUUIV1Annotator(
                    "mock",
                    endpoint,
                    new DUUIV1Config(1, "_InitialView", "_InitialView", Map.of()),
                    new DUUIV1Annotator.Documentation("mock", "1", "mock", "java", Map.of(), Map.of()),
                    TypeSystemDescriptionFactory.createTypeSystemDescription(),
                    communicationLayer,
                    cas -> cas.setDocumentText("processed"));
            JCas cas = JCasFactory.createJCas();

            annotator.process(DUUIArtifact.of(cas));

            assertEquals("processed", cas.getDocumentText());
            assertEquals(endpoint, annotator.endpoint());

            DUUIChannel<String> process = new DUUIChannel<>(
                    endpoint,
                    DUUIHttpMethod.POST,
                    "/v1/process",
                    (value, output) -> output.write(value.getBytes(StandardCharsets.UTF_8)),
                    (value, input) -> new String(input.readAllBytes(), StandardCharsets.UTF_8));

            assertEquals("processed:input", process.request("input"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void dockerStyleEnvironmentHandlesAreProxiesNotComponents() throws Exception {
        FakeVirtualizationClient client = new FakeVirtualizationClient();
        DUUIContainerImage image = client.image("duui/mock:latest");
        DUUIContainer container = image.run(List.of("serve"));

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

    private static final class SimpleCommunicationLayer implements DUUICommunicationLayer {
        @Override
        public void serialize(JCas sourceCas, OutputStream output, Map<String, String> parameters, String sourceView)
                throws CASException {
            try {
                output.write(sourceCas.getDocumentText().getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void deserialize(JCas targetCas, InputStream input, String targetView) throws CASException {
            try {
                targetCas.setDocumentText(new String(input.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public DUUICommunicationLayer copy() {
            return new SimpleCommunicationLayer();
        }
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

    private static final class FakeImage extends DUUIContainerImage {
        private FakeImage(String reference) {
            super(org.texttechnologylab.duui.clients.handle.DUUIAddress.parse("docker-image:/" + reference), reference, 0, Instant.now());
        }

        @Override
        public DUUIContainer run(List<String> command) {
            return new FakeContainer("container-1", this);
        }

        @Override
        public DUUIContainerImage pull() {
            return this;
        }

        @Override
        public DUUIContainerImage push() {
            return this;
        }

        @Override
        public DUUIContainerImage build(String context) {
            return this;
        }
    }

    private static final class FakeContainer extends DUUIContainer {
        private FakeContainer(String id, DUUIContainerImage image) {
            super(org.texttechnologylab.duui.clients.handle.DUUIAddress.parse("docker-container:/" + id), id, image, Instant.now());
        }

        @Override
        public boolean running() {
            return true;
        }

        @Override
        public DUUIContainer start() throws DUUIVirtualizationException {
            return this;
        }

        @Override
        public DUUIContainer stop() throws DUUIVirtualizationException {
            return this;
        }

        @Override
        public DUUIContainer restart() throws DUUIVirtualizationException {
            return this;
        }

        @Override
        public void delete() throws DUUIVirtualizationException {
        }
    }
}
