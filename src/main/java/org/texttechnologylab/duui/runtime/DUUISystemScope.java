package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutionContext;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDirector;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrator;
import org.texttechnologylab.duui.orchestration.DUUIOrchestratorConfig;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIScheduler;
import org.texttechnologylab.duui.orchestration.scheduling.DUUITypeDirector;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class DUUISystemScope implements AutoCloseable {
    private final String id;
    private final Map<String, DUUIPipeline> pipelines = new LinkedHashMap<>();
    private DUUIExecutor executor;
    private DUUIScheduler scheduler = new DUUIScheduler();
    private DUUIDirector director = new DUUITypeDirector();
    private DUUIOrchestratorConfig orchestratorConfig = DUUIOrchestratorConfig.DEFAULT;
    private DUUIEventService eventService;
    private boolean closed;

    DUUISystemScope(String id) {
        this.id = Objects.requireNonNull(id, "id");
    }

    public DUUIExecutorScope executor() {
        return new DUUIExecutorScope(this);
    }

    public DUUISchedulerScope scheduler() {
        return new DUUISchedulerScope(this);
    }

    public DUUIOrchestratorScope orchestrator() {
        return new DUUIOrchestratorScope(this);
    }

    public DUUIPipelineScope pipeline(String id) {
        return new DUUIPipelineScope(this, id);
    }

    public DUUISystemScope events(DUUIEventService eventService) {
        this.eventService = eventService;
        return this;
    }

    void executor(DUUIExecutor executor) {
        this.executor = executor;
    }

    void scheduler(DUUIScheduler scheduler) {
        this.scheduler = scheduler == null ? new DUUIScheduler() : scheduler;
    }

    void director(DUUIDirector director) {
        this.director = director == null ? new DUUITypeDirector() : director;
    }

    void orchestratorConfig(DUUIOrchestratorConfig orchestratorConfig) {
        this.orchestratorConfig = orchestratorConfig == null ? DUUIOrchestratorConfig.DEFAULT : orchestratorConfig;
    }

    void pipeline(DUUIPipeline pipeline) {
        pipelines.put(pipeline.id(), pipeline);
    }

    public DUUIPipeline pipelineById(String id) {
        DUUIPipeline pipeline = pipelines.get(id);
        if (pipeline == null) {
            throw new IllegalArgumentException("Unknown DUUI pipeline: " + id);
        }
        return pipeline;
    }

    public DUUIOrchestrationResult run(String pipelineId, Collection<DUUIArtifact<?>> artifacts) {
        DUUIExecutor effectiveExecutor = executor == null ? new DUUIExecutor(id) : executor;
        if (executor == null) {
            executor = effectiveExecutor;
        }
        return new DUUIOrchestrator(
                pipelineById(pipelineId),
                scheduler,
                director,
                effectiveExecutor,
                orchestratorConfig
        ).run(artifacts, rootContext());
    }

    public DUUIOrchestrationResult run(String pipelineId) {
        DUUIExecutor effectiveExecutor = executor == null ? new DUUIExecutor(id) : executor;
        if (executor == null) {
            executor = effectiveExecutor;
        }
        return new DUUIOrchestrator(
                pipelineById(pipelineId),
                scheduler,
                director,
                effectiveExecutor,
                orchestratorConfig
        ).run(rootContext());
    }

    public DUUIOrchestrationResult run(String pipelineId, DUUIArtifact<?> artifact) {
        return run(pipelineId, java.util.List.of(artifact));
    }

    public String id() {
        return id;
    }

    private DUUIExecutionContext rootContext() {
        DUUIExecutionContext context = new DUUIExecutionContext();
        if (eventService != null) {
            context.eventService(eventService);
        }
        return context;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (DUUIPipeline pipeline : pipelines.values()) {
            for (var checkpoint : pipeline.checkpoints()) {
                for (var stage : checkpoint.stages()) {
                    for (var component : stage.components()) {
                        try {
                            component.close();
                        } catch (Exception e) {
                            throw new IllegalStateException("Failed to close DUUI component " + component.id(), e);
                        }
                    }
                }
            }
        }
        if (executor != null) {
            executor.close();
        }
    }
}
