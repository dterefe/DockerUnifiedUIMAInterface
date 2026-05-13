package org.texttechnologylab.duui.runtime;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.event.DUUIEventSink;
import org.texttechnologylab.duui.pipeline.DUUIComponent;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;
import org.texttechnologylab.duui.protocol.v1.DUUIV1TelemetryConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class DUUIV1ComponentBuilder implements DUUIStageContribution {
    private final DUUIStageScope<?> stage;
    private final String id;
    private String endpoint;
    private int scale = 1;
    private int concurrency = 1;
    private String sourceView = "_InitialView";
    private String targetView = "_InitialView";
    private Map<String, String> parameters = Map.of();
    private boolean telemetryEnabled;
    private int telemetryTtlMinutes = 5;
    private DUUIEventSink telemetrySink;

    DUUIV1ComponentBuilder(DUUIStageScope<?> stage, String id) {
        this.stage = stage;
        this.id = id;
    }

    public DUUIV1ComponentBuilder remote() {
        return this;
    }

    public DUUIV1ComponentBuilder endpoint(String endpoint) {
        this.endpoint = endpoint;
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
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException("DUUI v1 component requires a remote endpoint: " + id);
        }
        try {
            List<DUUIV1Annotator> annotators = new ArrayList<>();
            DUUIV1Config config = new DUUIV1Config(
                    concurrency,
                    sourceView,
                    targetView,
                    parameters,
                    telemetryEnabled
                            ? new DUUIV1TelemetryConfig(true, telemetryTtlMinutes, telemetrySink)
                            : DUUIV1TelemetryConfig.disabled()
            );
            for (int i = 0; i < scale; i++) {
                annotators.add(new DUUIV1Annotator(
                        id + "-replica-" + i,
                        new DUUIHttpEndpoint(URI.create(endpoint), HttpClient.newHttpClient()),
                        config
                ));
            }
            DUUIComponent<JCas> component = DUUIComponent.v1(id, annotators);
            stage.jcasComponent(component);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build DUUI v1 component: " + id, e);
        }
    }
}
