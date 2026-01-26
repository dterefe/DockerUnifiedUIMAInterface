package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.components.instances.base;

import java.net.URI;
import java.util.Optional;

import org.apache.uima.analysis_engine.AnalysisEngine;

@SuppressWarnings("unchecked")
public abstract class AbstractInstance<SELF extends AbstractInstance<SELF>> {
    protected final SELF self() {
        return (SELF) this;
    }

    protected String instanceId;
    protected URI endpoint;
    protected String containerId;
    protected AnalysisEngine engine;

    public final SELF withInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return self();
    }

    public final SELF withEndpoint(URI endpoint) {
        this.endpoint = endpoint;
        return self();
    }

    public final SELF withContainerId(String containerId) {
        this.containerId = containerId;
        return self();
    }

    public final SELF withEngine(AnalysisEngine engine) {
        this.engine = engine;
        return self();
    }

    public final String instanceId() {
        return Optional.ofNullable(instanceId).orElseThrow();
    }

    public final URI endpoint() {
        return Optional.ofNullable(endpoint).orElseThrow();
    }

    public final String containerId() {
        return Optional.ofNullable(containerId).orElseThrow();
    }

    public final AnalysisEngine engine() {
        return Optional.ofNullable(engine).orElseThrow();
    }
}
