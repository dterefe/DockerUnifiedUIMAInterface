package org.texttechnologylab.duui.runtime;

import org.apache.uima.jcas.JCas;
import org.apache.uima.fit.factory.JCasFactory;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPodmanDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIKubernetesDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIV1Driver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.duui.event.DUUIEventSink;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.protocol.v1.DUUIV1TelemetryConfig;

import java.util.Arrays;
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
    private int telemetrySampleIntervalMs = 500;
    private boolean imageFetching;
    private boolean gpu;
    private boolean runningAfterDestroy;
    private long timeoutSeconds = 3600;
    private List<String> labels = List.of();
    private String contentType = "application/octet-stream";
    private boolean virtualThreads;

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

    public DUUIV1ComponentBuilder telemetrySampleIntervalMs(int sampleIntervalMs) {
        this.telemetrySampleIntervalMs = Math.max(100, sampleIntervalMs);
        this.telemetryEnabled = true;
        return this;
    }

    public DUUIV1ComponentBuilder virtualThreads() {
        this.virtualThreads = true;
        return this;
    }

    public DUUIV1ComponentBuilder virtualThreads(boolean virtualThreads) {
        this.virtualThreads = virtualThreads;
        return this;
    }

    public DUUIV1ComponentBuilder contentType(String contentType) {
        this.contentType = contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType;
        return this;
    }

    /**
     * Unified instantiation path — everything goes through {@code driver.instantiateV2()}.
     * <p>
     * Determines the environment (remote/podman/kubernetes), creates the appropriate
     * {@link DUUIV1Driver}, builds a {@link DUUIPipelineComponent}, and calls
     * {@link DUUIV1Driver#instantiateV2(DUUIPipelineComponent, JCas, boolean, AtomicBoolean)}.
     * The returned {@link DUUIComponent}{@code <JCas>} is added directly to the stage.
     * No alternative paths exist.
     */
    @Override
    public void contribute() {
        try {
            DUUIV1Driver driver;
            DUUIPipelineComponent component;

            switch (environment) {
                case REMOTE -> {
                    if (endpoint == null || endpoint.isBlank()) {
                        throw new IllegalStateException("DUUI v1 remote component requires an endpoint: " + id);
                    }
                    driver = new DUUIRemoteDriver();
                    component = new DUUIPipelineComponent();
                    component.withUrl(endpoint);
                }
                case PODMAN -> {
                    if (image == null || image.isBlank()) {
                        throw new IllegalStateException("DUUI v1 Podman component requires an image: " + id);
                    }
                    driver = new DUUIPodmanDriver();
                    component = new DUUIPodmanDriver.Component(image)
                            .withImageFetching(imageFetching)
                            .withGPU(gpu)
                            .withRunningAfterDestroy(runningAfterDestroy)
                            .build();
                }
                case KUBERNETES -> {
                    if (image == null || image.isBlank()) {
                        throw new IllegalStateException("DUUI v1 Kubernetes component requires an image: " + id);
                    }
                    driver = new DUUIKubernetesDriver();
                    DUUIKubernetesDriver.Component builder = new DUUIKubernetesDriver.Component(image);
                    if (!labels.isEmpty()) {
                        builder.withLabels(labels);
                    }
                    component = builder.build();
                }
                default -> throw new IllegalStateException("Unknown environment: " + environment);
            }

            // Apply common configuration
            component.withName(id);
            component.withScale(scale);
            component.withWorkers(concurrency);
            component.withSourceView(sourceView);
            component.withTargetView(targetView);
            parameters.forEach(component::withParameter);
            component.withTimeout(timeoutSeconds);

            // --- Unified instantiation path: driver.instantiateV2 ---
            driver.setLuaContext(LuaConsts.getJSON());
            driver.withVirtualThreads(virtualThreads);
            driver.withV1Transport(true, contentType);
            driver.withV1Telemetry(telemetryConfig());
            DUUIComponent<JCas> duuiComponent = driver.instantiateV2(
                    component, healthCas(), false, new AtomicBoolean(false));

            if (duuiComponent == null) {
                throw new IllegalStateException("instantiateV2 returned null for component: " + id);
            }

            stage.jcasComponent(duuiComponent);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build DUUI v1 component: " + id, e);
        }
    }

    private static JCas healthCas() throws Exception {
        JCas cas = JCasFactory.createJCas();
        cas.setDocumentLanguage("en");
        cas.setDocumentText("DUUI health check.");
        return cas;
    }

    private DUUIV1TelemetryConfig telemetryConfig() {
        if (!telemetryEnabled) {
            return DUUIV1TelemetryConfig.disabled();
        }
        return new DUUIV1TelemetryConfig(
                true,
                telemetryTtlMinutes,
                telemetrySink,
                telemetrySampleIntervalMs);
    }
}
