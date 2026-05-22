package org.texttechnologylab.duui.protocol.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.texttechnologylab.duui.clients.http.DUUIChannel;
import org.texttechnologylab.duui.clients.http.DUUIDeserializer;
import org.texttechnologylab.duui.clients.http.DUUIHttpMethod;
import org.texttechnologylab.duui.clients.http.DUUISignal;
import org.texttechnologylab.duui.communication.DUUICommunicationLayer;
import org.texttechnologylab.duui.communication.DUUILuaCommunicationLayer;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.XMLInputSource;

import org.texttechnologylab.duui.clients.http.DUUISerializer;
import org.texttechnologylab.duui.clients.http.IDUUIEndpoint;
import org.texttechnologylab.duui.event.DUUIEventContext;
import org.texttechnologylab.duui.event.DUUIEventScope;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUIRemoteEventStream;
import org.texttechnologylab.duui.pipeline.component.DUUIAnnotator;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class DUUIV1Annotator implements DUUIAnnotator<JCas> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @FunctionalInterface
    public interface Processor {
        void process(JCas cas) throws Exception;
    }

    private final GID gid;
    private final DUUITraits traits;
    private final String id;
    private final IDUUIEndpoint endpointHandle;
    private final DUUIV1Config config;
    private final Documentation documentation;
    private final TypeSystemDescription typesystem;
    private final DUUICommunicationLayer communicationLayer;
    private final Processor processor;
    private final DUUISignal<Documentation> documentationSignal;
    private final DUUISignal<TypeSystemDescription> typesystemSignal;
    private final DUUISignal<DUUICommunicationLayer> communicationLayerSignal;
    private final DUUIChannel<JCas> processChannel;
    private final DUUIRemoteEventStream eventStream;

    public DUUIV1Annotator(String id, IDUUIEndpoint endpoint, DUUIV1Config config) throws Exception {
        this.gid = GID.create();
        this.traits = DUUITraits.empty();
        this.id = Objects.requireNonNull(id, "id");
        this.endpointHandle = Objects.requireNonNull(endpoint, "endpoint");
        this.config = Objects.requireNonNull(config, "config");
        this.documentationSignal = documentationSignal(endpoint);
        this.typesystemSignal = typesystemSignal(endpoint);
        this.communicationLayerSignal = communicationLayerSignal(endpoint);
        this.documentation = documentationSignal.request();
        this.typesystem = typesystemSignal.request();
        this.communicationLayer = communicationLayerSignal.request();
        this.eventStream = DUUIRemoteEventStream.connect(endpoint, config.telemetry(), id);
        this.processChannel = processChannel(endpoint, communicationLayer, config);
        this.processor = cas -> processChannel.request(cas);
    }

    public DUUIV1Annotator(
        String id,
        IDUUIEndpoint endpoint,
        DUUIV1Config config,
        Documentation documentation,
        TypeSystemDescription typesystem,
        DUUICommunicationLayer communicationLayer,
        Processor processor
    ) {
        this.gid = GID.create();
        this.traits = DUUITraits.empty();
        this.id = Objects.requireNonNull(id, "id");
        this.endpointHandle = Objects.requireNonNull(endpoint, "endpoint");
        this.config = Objects.requireNonNull(config, "config");
        this.documentation = Objects.requireNonNull(documentation, "documentation");
        this.typesystem = Objects.requireNonNull(typesystem, "typesystem");
        this.communicationLayer = Objects.requireNonNull(communicationLayer, "communicationLayer");
        this.processor = Objects.requireNonNull(processor, "processor");
        this.documentationSignal = documentationSignal(endpoint);
        this.typesystemSignal = typesystemSignal(endpoint);
        this.communicationLayerSignal = communicationLayerSignal(endpoint);
        this.eventStream = DUUIRemoteEventStream.connect(endpoint, config.telemetry(), id);
        this.processChannel = processChannel(endpoint, communicationLayer, config);
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

    public IDUUIEndpoint endpoint() {
        return endpointHandle;
    }

    public DUUIV1Config config() {
        return config;
    }

    public Documentation documentation() {
        return documentation;
    }

    public TypeSystemDescription typesystem() {
        return typesystem;
    }

    public DUUICommunicationLayer communicationLayer() {
        return communicationLayer;
    }

    public void serialize(JCas cas, OutputStream stream, Map<String, String> parameters, String sourceView) throws CASException {
        communicationLayer.serialize(cas, stream, parameters, sourceView);
    }

    public void deserialize(JCas cas, InputStream stream, String targetView) throws CASException {
        communicationLayer.deserialize(cas, stream, targetView);
    }

    @Override
    public DUUIArtifact<JCas> process(DUUIArtifact<JCas> artifact) throws Exception {
        DUUIEventService service = DUUIEventService.current();
        service.logger("duui.v1").info("Sending artifact " + artifact.id() + " to v1 annotator " + id());
        DUUIEventScope scope = service.scope("v1.process");
        try {
            processor.process(artifact.payload());
            return artifact;
        } catch (Exception error) {
            scope.fail(error);
            throw error;
        } finally {
            scope.close();
        }
    }

    private DUUISignal<Documentation> documentationSignal(IDUUIEndpoint endpoint) {
        return new DUUISignal<>(endpoint, DUUIHttpMethod.GET, "/v1/documentation", documentationDeserializer());
    }

    private DUUISignal<TypeSystemDescription> typesystemSignal(IDUUIEndpoint endpoint) {
        return new DUUISignal<>(endpoint, DUUIHttpMethod.GET, "/v1/typesystem", typesystemDeserializer());
    }

    private DUUISignal<DUUICommunicationLayer> communicationLayerSignal(IDUUIEndpoint endpoint) {
        return new DUUISignal<>(endpoint, DUUIHttpMethod.GET, "/v1/communication_layer", communicationLayerDeserializer());
    }

    private DUUIChannel<JCas> processChannel(
        IDUUIEndpoint endpoint,
        DUUICommunicationLayer communicationLayer,
        DUUIV1Config config
    ) {
        DUUISerializer<JCas> serializer = processSerializer(communicationLayer, config);
        DUUIChannel.ResponseApplier<JCas> applier = processDeserializer(communicationLayer, config);
        return new DUUIChannel<>(endpoint, DUUIHttpMethod.POST, "/v1/process", serializer, applier, telemetryCustomizer(config));
    }

    private DUUIChannel.RequestCustomizer<JCas> telemetryCustomizer(DUUIV1Config config) {
        if (!config.telemetry().enabled()) {
            return new DUUIChannel.RequestCustomizer<>() {};
        }
        return new DUUIChannel.RequestCustomizer<>() {
            @Override
            public URI uri(URI baseUri, JCas value) {
                try {
                    DUUIEventContext context = DUUIEventService.current().currentContext();
                    String eventContext = context.toRemoteContextMap().entrySet().stream()
                            .map(entry -> entry.getKey() + "=" + entry.getValue())
                            .collect(java.util.stream.Collectors.joining(","));
                    String encoded = URLEncoder.encode(eventContext, StandardCharsets.UTF_8);
                    String separator = baseUri.getQuery() == null ? "?" : "&";
                    return URI.create(baseUri + separator + "event-context=" + encoded);
                } catch (Exception ignored) {
                    return baseUri;
                }
            }

            @Override
            public void customize(java.net.http.HttpRequest.Builder builder, JCas value) {
                builder.header("x-request-id", UUID.randomUUID().toString());
            }
        };
    }

    private DUUIDeserializer<Documentation> documentationDeserializer() {
        return input -> MAPPER.readValue(input, Documentation.class);
    }

    private DUUIDeserializer<TypeSystemDescription> typesystemDeserializer() {
        return input -> UIMAFramework.getXMLParser().parseTypeSystemDescription(new XMLInputSource(input, null));
    }

    private DUUIDeserializer<DUUICommunicationLayer> communicationLayerDeserializer() {
        return input -> new DUUILuaCommunicationLayer(new String(input.readAllBytes(), StandardCharsets.UTF_8));
    }

    private DUUISerializer<JCas> processSerializer(DUUICommunicationLayer communicationLayer, DUUIV1Config config) {
        return (cas, output) -> communicationLayer.serialize(cas, output, config.parameters(), config.sourceView());
    }

    private DUUIChannel.ResponseApplier<JCas> processDeserializer(DUUICommunicationLayer communicationLayer, DUUIV1Config config) {
        return (cas, input) -> {
            communicationLayer.deserialize(cas, input, config.targetView());
            return cas;
        };
    }

    public record Documentation(
        String annotator_name,
        String version,
        String description,
        String implementation_lang,
        Map<String, Object> meta,
        Map<String, Object> parameters
    ) {
    }
}
