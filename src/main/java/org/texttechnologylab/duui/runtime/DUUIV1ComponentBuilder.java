package org.texttechnologylab.duui.runtime;

import org.apache.uima.jcas.JCas;
import org.apache.uima.fit.factory.JCasFactory;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPodmanDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIKubernetesDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.event.DUUIEventSink;
import org.texttechnologylab.duui.pipeline.DUUIComponent;
import org.texttechnologylab.duui.pipeline.DUUINode;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;
import org.texttechnologylab.duui.protocol.v1.DUUIV1TelemetryConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DUUIV1ComponentBuilder implements DUUIStageContribution {
    private enum Environment {
        REMOTE,
        PODMAN,
        KUBERNETES
    }

    private final DUUIStageScope<?> stage;
    private final String id;
    private Environment environment = Environment.REMOTE;
    private String endpoint;
    private String image;
    private int scale = 1;
    private int concurrency = 1;
    private String sourceView = "_InitialView";
    private String targetView = "_InitialView";
    private Map<String, String> parameters = Map.of();
    private boolean telemetryEnabled;
    private int telemetryTtlMinutes = 5;
    private DUUIEventSink telemetrySink;
    private boolean imageFetching;
    private boolean gpu;
    private boolean runningAfterDestroy;
    private long timeoutSeconds = 3600;
    private List<String> labels = List.of();

    DUUIV1ComponentBuilder(DUUIStageScope<?> stage, String id) {
        this.stage = stage;
        this.id = id;
    }

    public DUUIV1ComponentBuilder remote() {
        this.environment = Environment.REMOTE;
        return this;
    }

    public DUUIV1ComponentBuilder podman() {
        this.environment = Environment.PODMAN;
        return this;
    }

    public DUUIV1ComponentBuilder kubernetes() {
        this.environment = Environment.KUBERNETES;
        return this;
    }

    public DUUIV1ComponentBuilder endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public DUUIV1ComponentBuilder image(String image) {
        this.image = image;
        return this;
    }

    public DUUIV1ComponentBuilder scale(int scale) {
        this.scale = Math.max(1, scale);
        return this;
    }

    public DUUIV1ComponentBuilder concurrency(int concurrency) {
        this.concurrency = Math.max(1, concurrency);
        return this;
    }

    public DUUIV1ComponentBuilder sourceView(String sourceView) {
        this.sourceView = sourceView == null ? "_InitialView" : sourceView;
        return this;
    }

    public DUUIV1ComponentBuilder targetView(String targetView) {
        this.targetView = targetView == null ? "_InitialView" : targetView;
        return this;
    }

    public DUUIV1ComponentBuilder parameters(Map<String, String> parameters) {
        this.parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
        return this;
    }

    public DUUIV1ComponentBuilder imageFetching() {
        this.imageFetching = true;
        return this;
    }

    public DUUIV1ComponentBuilder imageFetching(boolean imageFetching) {
        this.imageFetching = imageFetching;
        return this;
    }

    public DUUIV1ComponentBuilder gpu() {
        this.gpu = true;
        return this;
    }

    public DUUIV1ComponentBuilder gpu(boolean gpu) {
        this.gpu = gpu;
        return this;
    }

    public DUUIV1ComponentBuilder runningAfterDestroy() {
        this.runningAfterDestroy = true;
        return this;
    }

    public DUUIV1ComponentBuilder runningAfterDestroy(boolean runningAfterDestroy) {
        this.runningAfterDestroy = runningAfterDestroy;
        return this;
    }

    public DUUIV1ComponentBuilder timeoutSeconds(long timeoutSeconds) {
        this.timeoutSeconds = Math.max(1, timeoutSeconds);
        return this;
    }

    public DUUIV1ComponentBuilder labels(String... labels) {
        this.labels = labels == null ? List.of() : Arrays.stream(labels).filter(label -> label != null && !label.isBlank()).toList();
        return this;
    }

    public DUUIV1ComponentBuilder labels(List<String> labels) {
        this.labels = labels == null ? List.of() : List.copyOf(labels);
        return this;
    }

    public DUUIV1ComponentBuilder telemetry() {
        this.telemetryEnabled = true;
        return this;
    }

    public DUUIV1ComponentBuilder telemetry(boolean enabled) {
        this.telemetryEnabled = enabled;
        return this;
    }

    public DUUIV1ComponentBuilder telemetrySink(DUUIEventSink sink) {
        this.telemetrySink = sink;
        this.telemetryEnabled = true;
        return this;
    }

    public DUUIV1ComponentBuilder telemetryTtlMinutes(int ttlMinutes) {
        this.telemetryTtlMinutes = ttlMinutes;
        return this;
    }

    @Override
    public void contribute() {
        try {
            switch (environment) {
                case REMOTE -> contributeRemote();
                case PODMAN -> contributePodman();
                case KUBERNETES -> contributeKubernetes();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build DUUI v1 component: " + id, e);
        }
    }

    private void contributeRemote() throws Exception {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException("DUUI v1 remote component requires an endpoint: " + id);
        }
        List<String> endpoints = new ArrayList<>();
        for (int i = 0; i < scale; i++) {
            endpoints.add(endpoint);
        }
        stage.jcasComponent(DUUIComponent.v1(id, annotators(endpoints, null)));
    }

    private void contributePodman() throws Exception {
        if (image == null || image.isBlank()) {
            throw new IllegalStateException("DUUI v1 Podman component requires an image: " + id);
        }
        DUUIPodmanDriver driver = new DUUIPodmanDriver();
        driver.setLuaContext(LuaConsts.getJSON());
        DUUIPipelineComponent component = new DUUIPodmanDriver.Component(image)
                .withScale(scale)
                .withWorkers(1)
                .withImageFetching(imageFetching)
                .withGPU(gpu)
                .withRunningAfterDestroy(runningAfterDestroy)
                .withSourceView(sourceView)
                .withTargetView(targetView)
                .build()
                .withTimeout(timeoutSeconds);
        parameters.forEach(component::withParameter);
        String uuid = driver.instantiate(component, healthCas(), true, new AtomicBoolean(false));
        List<String> endpoints = driver.getEndpointUrls(uuid);
        if (endpoints.isEmpty()) {
            driver.destroy(uuid);
            throw new IllegalStateException("Podman component did not expose any DUUI v1 endpoint: " + id);
        }
        stage.jcasComponent(new DUUIComponent<>(id, nodes(annotators(endpoints, id + "-podman")), () -> driver.destroy(uuid)));
    }

    private void contributeKubernetes() throws Exception {
        if (image == null || image.isBlank()) {
            throw new IllegalStateException("DUUI v1 Kubernetes component requires an image: " + id);
        }
        DUUIKubernetesDriver driver = new DUUIKubernetesDriver();
        driver.setLuaContext(LuaConsts.getJSON());
        DUUIKubernetesDriver.Component builder = new DUUIKubernetesDriver.Component(image)
                .withScale(scale)
                .withSourceView(sourceView)
                .withTargetView(targetView);
        if (!labels.isEmpty()) {
            builder.withLabels(labels);
        }
        DUUIPipelineComponent component = builder.build().withTimeout(timeoutSeconds);
        parameters.forEach(component::withParameter);
        String uuid = driver.instantiate(component, healthCas(), true, new AtomicBoolean(false));
        List<String> endpoints = driver.getEndpointUrls(uuid);
        if (endpoints.isEmpty()) {
            driver.destroy(uuid);
            throw new IllegalStateException("Kubernetes component did not expose any DUUI v1 endpoint: " + id);
        }
        stage.jcasComponent(new DUUIComponent<>(id, nodes(annotators(endpoints, id + "-kubernetes")), () -> driver.destroy(uuid)));
    }

    private List<DUUIV1Annotator> annotators(List<String> endpoints, String replicaPrefix) throws Exception {
        List<DUUIV1Annotator> annotators = new ArrayList<>();
        DUUIV1Config config = config();
        int replica = 0;
        for (String endpoint : endpoints) {
            annotators.add(new DUUIV1Annotator(
                    (replicaPrefix == null ? id : replicaPrefix) + "-replica-" + replica++,
                    new DUUIHttpEndpoint(URI.create(endpoint), HttpClient.newHttpClient()),
                    config
            ));
        }
        return annotators;
    }

    private List<DUUINode<JCas>> nodes(List<DUUIV1Annotator> annotators) {
        List<DUUINode<JCas>> nodes = new ArrayList<>();
        int slot = 0;
        for (DUUIV1Annotator annotator : annotators) {
            for (int i = 0; i < annotator.config().concurrency(); i++) {
                nodes.add(DUUINode.v1(id + "-slot-" + slot++, annotator));
            }
        }
        return nodes;
    }

    private DUUIV1Config config() {
        return new DUUIV1Config(
                concurrency,
                sourceView,
                targetView,
                parameters,
                telemetryEnabled
                        ? new DUUIV1TelemetryConfig(true, telemetryTtlMinutes, telemetrySink)
                        : DUUIV1TelemetryConfig.disabled()
        );
    }

    private static JCas healthCas() throws Exception {
        JCas cas = JCasFactory.createJCas();
        cas.setDocumentLanguage("en");
        cas.setDocumentText("DUUI health check.");
        return cas;
    }
}
