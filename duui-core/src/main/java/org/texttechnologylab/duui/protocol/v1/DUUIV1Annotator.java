package org.texttechnologylab.duui.protocol.v1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.uima.UIMAFramework;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.XMLInputSource;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.clients.http.DUUIChannel;
import org.texttechnologylab.duui.clients.http.DUUIDeserializer;
import org.texttechnologylab.duui.clients.http.DUUIHttpResponse;
import org.texttechnologylab.duui.clients.http.DUUIHttpMethod;
import org.texttechnologylab.duui.clients.http.DUUIRelay;
import org.texttechnologylab.duui.clients.http.DUUISignal;
import org.texttechnologylab.duui.clients.http.DUUIStreamBodyHandler;
import org.texttechnologylab.duui.communication.DUUICommunicationLayer;
import org.texttechnologylab.duui.communication.DUUILuaCommunicationLayer;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.event.DUUIEvent;
import org.texttechnologylab.duui.event.DUUIEventContext;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.filesystem.DUUIStream;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.pipeline.component.DUUIAnnotator;
import org.texttechnologylab.duui.timelines.DUUIFlow;
import org.texttechnologylab.duui.timelines.DUUIStatus;
import org.texttechnologylab.duui.timelines.Phase;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public final class DUUIV1Annotator implements DUUIAnnotator<JCas> {
    private static final ObjectMapper JSON = new ObjectMapper();

    public record DUUIPipe(
            DUUIRelay<JCas> inputRelay,
            DUUIRelay<DUUIHttpResponse> outputRelay
    ) {
        public DUUIPipe {
            Objects.requireNonNull(inputRelay, "inputRelay");
            Objects.requireNonNull(outputRelay, "outputRelay");
        }
    }

    private final GID gid;
    private final DUUITraits traits;
    private final String id;
    private final org.texttechnologylab.duui.clients.http.IDUUIEndpoint endpointHandle;
    private final DUUIV1Config config;
    private final Documentation documentation;
    private final TypeSystemDescription typesystem;
    private final DUUICommunicationLayer communicationLayer;
    private final DUUISignal<Documentation> documentationSignal;
    private final DUUISignal<TypeSystemDescription> typesystemSignal;
    private final DUUISignal<DUUICommunicationLayer> communicationLayerSignal;
    private final DUUISignal<DUUIStream<DUUIEvent>> eventSignal;
    private final DUUIChannel<JCas> processChannel;
    private final BlockingQueue<DUUIPipe> processPipes;

    public DUUIV1Annotator(
            String id,
            org.texttechnologylab.duui.clients.http.IDUUIEndpoint endpoint,
            DUUIV1Config config
    ) throws Exception {
        long initStart = System.currentTimeMillis();
        this.gid = GID.create();
        this.traits = DUUITraits.empty();
        this.id = Objects.requireNonNull(id, "id");
        this.endpointHandle = Objects.requireNonNull(endpoint, "endpoint");
        this.config = Objects.requireNonNull(config, "config");
        DUUIEventService.current().logger("duui.v1").info("Initializing v1 annotator id=" + id + " endpoint=" + endpoint.uri());
        this.documentationSignal = new DUUISignal<>(endpoint, DUUIHttpMethod.GET, "/v1/documentation",
                (DUUIDeserializer<Documentation>) input -> JSON.readValue(input, Documentation.class));
        this.typesystemSignal = new DUUISignal<>(endpoint, DUUIHttpMethod.GET, "/v1/typesystem",
                (DUUIDeserializer<TypeSystemDescription>) input -> UIMAFramework.getXMLParser().parseTypeSystemDescription(new XMLInputSource(input, null)));
        this.communicationLayerSignal = new DUUISignal<>(endpoint, DUUIHttpMethod.GET, "/v1/communication_layer",
                (DUUIDeserializer<DUUICommunicationLayer>) input -> new DUUILuaCommunicationLayer(new String(input.readAllBytes(), StandardCharsets.UTF_8)));
        DUUIV1TelemetryConfig telemetry = config.telemetry() == null ? DUUIV1TelemetryConfig.disabled() : config.telemetry();
        this.eventSignal = new DUUISignal<>(
                endpoint,
                DUUIHttpMethod.GET,
                "/v2/events?ttl_minutes=" + URLEncoder.encode(String.valueOf(telemetry.ttlMinutes()), StandardCharsets.UTF_8)
                        + "&annotator_id=" + URLEncoder.encode(id, StandardCharsets.UTF_8)
                        + "&replica_id=" + URLEncoder.encode(id, StandardCharsets.UTF_8),
                new DUUIStreamBodyHandler<>(DUUIEvent.remoteDeserializer()));
        this.documentation = requestDocumentationOptional(endpoint);
        DUUIEventService.current().logger("duui.v1").debug("Loaded v1 documentation id=" + id + " name=" + documentation.annotator_name() + " version=" + documentation.version());
        this.typesystem = typesystemSignal.request();
        DUUIEventService.current().logger("duui.v1").debug("Loaded v1 typesystem id=" + id);
        this.communicationLayer = communicationLayerSignal.request();
        DUUIEventService.current().logger("duui.v1").debug("Loaded v1 communication layer id=" + id);
        this.processChannel = new DUUIChannel<>(
                endpoint,
                DUUIHttpMethod.POST,
                "/v1/process",
                new DUUIChannel.RequestCustomizer<>() {
                    @Override
                    public URI uri(URI baseUri, JCas value) {
                        if (!config.telemetry().enabled()) {
                            return baseUri;
                        }
                        try {
                            DUUIEventContext context = DUUIEventService.current().currentContext();
                            String encodedContextSource = context.toRemoteContextMap().entrySet().stream()
                                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                                    .collect(java.util.stream.Collectors.joining(","));
                            String encoded = URLEncoder.encode(encodedContextSource, StandardCharsets.UTF_8);
                            String separator = baseUri.getQuery() == null ? "?" : "&";
                            return URI.create(baseUri + separator + "event-context=" + encoded);
                        } catch (Exception ignored) {
                            return baseUri;
                        }
                    }

                    @Override
                    public void customize(HttpRequest.Builder builder, JCas value) {
                        header(builder, "Content-Type", config.contentType());
                        if (!config.telemetry().enabled()) {
                            return;
                        }
                        DUUIEventContext context = DUUIEventService.current().currentContext();
                        String requestId = UUID.randomUUID().toString();
                        header(builder, "x-request-id", requestId);
                        header(builder, "X-DUUI-Request-Id", requestId);
                        header(builder, "X-DUUI-Orchestrator-Id", context.orchestratorId());
                        header(builder, "X-DUUI-Artifact-Id", context.artifactId());
                        header(builder, "X-DUUI-Component-Id", context.componentId());
                        header(builder, "X-DUUI-Replica-Id", firstPresent(context.nodeId(), context.annotatorId(), id));
                        header(builder, "X-DUUI-Annotator-Id", firstPresent(context.annotatorId(), id));
                        header(builder, "X-DUUI-Machine-Id", context.workerId());
                        header(builder, "X-DUUI-Pipeline-Run-Id", context.taskId());
                        if (context.trace() != null) {
                            header(builder, "traceparent", "00-" + context.trace().traceId() + "-" + context.trace().spanId() + "-01");
                        }
                        try {
                            header(builder, "X-DUUI-Telemetry", JSON.writeValueAsString(Map.of(
                                    "sample_interval_ms", config.telemetry().sampleIntervalMs()
                            )));
                        } catch (Exception ignored) {
                        }
                    }
                },
                config.contentType());
        BlockingQueue<DUUIPipe> pipes = new LinkedBlockingQueue<>();
        for (int index = 0; index < config.concurrency(); index++) {
            pipes.offer(new DUUIPipe(new DUUIRelay<>(), new DUUIRelay<>()));
        }
        this.processPipes = pipes;
        long initDuration = System.currentTimeMillis() - initStart;
        DUUIEventService.current().logger("duui.v1").info("Initialized v1 annotator id=" + id + " duration_ms=" + initDuration);
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

    public org.texttechnologylab.duui.clients.http.IDUUIEndpoint endpoint() {
        return endpointHandle;
    }

    public DUUIV1Config config() {
        return config;
    }

    @Phase(value = DUUIStatus.READ, dispatch = DUUIDispatchMode.IO)
    public DUUIFlow<Documentation> documentation() {
        return DUUIFlow.dispatch(documentation);
    }

    @Phase(value = DUUIStatus.READ, dispatch = DUUIDispatchMode.IO)
    public DUUIFlow<TypeSystemDescription> typesystemDescription() {
        return DUUIFlow.dispatch(typesystem);
    }

    @Phase(value = DUUIStatus.READ, dispatch = DUUIDispatchMode.IO)
    public DUUIFlow<DUUICommunicationLayer> communicationLayer() {
        return DUUIFlow.dispatch(communicationLayer);
    }

    @Phase(value = DUUIStatus.READ, dispatch = DUUIDispatchMode.IO)
    public DUUIFlow<DUUIStream<DUUIEvent>> events() {
        try {
            return DUUIFlow.dispatch(eventSignal.request());
        } catch (Exception error) {
            return DUUIFlow.fail(error);
        }
    }

    public DUUIPipe borrowPipe() throws InterruptedException {
        return processPipes.take();
    }

    public void returnPipe(DUUIPipe pipe) throws Exception {
        if (pipe != null) {
            pipe.inputRelay().reset();
            pipe.outputRelay().reset();
            processPipes.offer(pipe);
        }
    }

    public void cancelPipe(DUUIPipe pipe, Throwable error) {
        if (pipe != null) {
            pipe.inputRelay().cancel(error);
            pipe.outputRelay().cancel(error);
        }
    }

    @Phase(value = DUUIStatus.SERIALIZE, dispatch = DUUIDispatchMode.IO)
    public DUUIFlow<Void> serialize(JCas cas, DUUIRelay<JCas> relay) {
        try (OutputStream output = relay.outputStream()) {
            communicationLayer.serialize(cas, output, config.parameters(), config.sourceView());
            return DUUIFlow.dispatch(null);
        } catch (Exception error) {
            relay.cancel(error);
            return DUUIFlow.fail(error);
        }
    }

    @Phase(value = DUUIStatus.DESERIALIZE, dispatch = DUUIDispatchMode.IO)
    public DUUIFlow<Void> deserialize(JCas cas, DUUIRelay<?> relay) {
        try (InputStream input = relay.inputStream()) {
            communicationLayer.deserialize(cas, input, config.targetView());
            return DUUIFlow.dispatch(null);
        } catch (Exception error) {
            relay.cancel(error);
            return DUUIFlow.fail(error);
        }
    }

    @Phase(value = DUUIStatus.ANALYSE, dispatch = DUUIDispatchMode.IO)
    public DUUIFlow<DUUIArtifact<JCas>> analyse(
            DUUIArtifact<JCas> artifact,
            DUUIRelay<JCas> inputRelay,
            DUUIRelay<DUUIHttpResponse> outputRelay
    ) {
        try {
            processChannel.request(inputRelay, outputRelay);
            return DUUIFlow.dispatch(artifact);
        } catch (Exception error) {
            return DUUIFlow.fail(error);
        }
    }

    private Documentation requestDocumentationOptional(org.texttechnologylab.duui.clients.http.IDUUIEndpoint endpoint) {
        try {
            return documentationSignal.request();
        } catch (Exception error) {
            DUUIEventService.current().logger("duui.v1").warn(
                    "Optional v1 documentation endpoint unavailable id=" + id
                            + " endpoint=" + endpoint.uri()
                            + " error=" + error.getMessage());
            return new Documentation(
                    id,
                    "unknown",
                    "Optional /v1/documentation endpoint unavailable.",
                    "unknown",
                    Map.of("endpoint", endpoint.uri().toString()),
                    Map.of());
        }
    }

    private static void header(HttpRequest.Builder builder, String name, String value) {
        if (value != null && !value.isBlank()) {
            builder.header(name, value);
        }
    }

    private static String firstPresent(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
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
