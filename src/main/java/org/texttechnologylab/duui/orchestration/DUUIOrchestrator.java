package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.orchestration.scheduling.DUUIDirector;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIScheduler;
import org.texttechnologylab.duui.orchestration.scheduling.DUUITypeDirector;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.exception.DUUIExecutionStatus;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutionContext;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorkerKind;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorkerRegistry;
import org.texttechnologylab.duui.orchestration.DUUITask;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;

public final class DUUIOrchestrator {
    private final String orchestratorId;
    private final DUUIPipeline pipeline;
    private final DUUIScheduler scheduler;
    private final DUUIDirector director;
    private final DUUIExecutor executor;
    private final DUUIOrchestratorConfig config;

    public DUUIOrchestrator(DUUIPipeline pipeline) {
        this(UUID.randomUUID().toString(), pipeline, new DUUIScheduler(), new DUUITypeDirector(), null, DUUIOrchestratorConfig.DEFAULT);
    }

    public DUUIOrchestrator(
            DUUIPipeline pipeline,
            DUUIScheduler scheduler,
            DUUIDirector director,
            DUUIExecutor executor,
            DUUIOrchestratorConfig config
    ) {
        this(executor == null ? UUID.randomUUID().toString() : executor.orchestratorId(), pipeline, scheduler, director, executor, config);
    }

    public DUUIOrchestrator(
            String orchestratorId,
            DUUIPipeline pipeline,
            DUUIScheduler scheduler,
            DUUIDirector director,
            DUUIExecutor executor,
            DUUIOrchestratorConfig config
    ) {
        this.orchestratorId = orchestratorId == null ? UUID.randomUUID().toString() : orchestratorId;
        this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
        this.scheduler = scheduler == null ? new DUUIScheduler() : scheduler;
        this.director = director == null ? new DUUITypeDirector() : director;
        this.executor = executor == null ? new DUUIExecutor(this.orchestratorId) : executor;
        this.config = config == null ? DUUIOrchestratorConfig.DEFAULT : config;
        DUUIWorkerRegistry.registerCurrentThread(this.executor.orchestratorId(), DUUIWorkerKind.ORIGIN, true);
    }

    public DUUIOrchestrationResult run(DUUIArtifact<?> artifact) {
        return run(java.util.List.of(artifact));
    }

    public DUUIOrchestrationResult run() {
        return run(new DUUIExecutionContext());
    }

    public DUUIOrchestrationResult run(DUUIExecutionContext rootContext) {
        DUUIExecutionContext effectiveRootContext = rootContext == null ? new DUUIExecutionContext() : rootContext;
        java.util.List<DUUIArtifact<?>> artifacts = new java.util.ArrayList<>();
        for (DUUIGenerator<?> generator : pipeline.generators()) {
            try {
                generator.generate(artifacts::add);
            } catch (Exception e) {
                throw new DUUIFrameworkStateException("DUUI generator failed before task scheduling.", e);
            }
        }
        return run(artifacts, effectiveRootContext);
    }

    public DUUIOrchestrationResult run(Collection<DUUIArtifact<?>> artifacts) {
        return run(artifacts, new DUUIExecutionContext());
    }

    public DUUIOrchestrationResult run(Collection<DUUIArtifact<?>> artifacts, DUUIExecutionContext rootContext) {
        DUUIOrchestrationResult orchestrationResult = new DUUIOrchestrationResult();
        Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues = initializeQueues();
        Map<String, DUUIExecutionContext> contexts = new LinkedHashMap<>();
        Map<String, DUUIArtifact<?>> parkedParents = new LinkedHashMap<>();
        Map<String, Integer> pendingChildren = new LinkedHashMap<>();
        DUUIExecutionContext initialContext = rootContext == null ? new DUUIExecutionContext() : rootContext;
        if (artifacts != null) {
            for (DUUIArtifact<?> artifact : artifacts.stream().filter(Objects::nonNull).toList()) {
                contexts.put(artifact.id(), initialContext.copyValues());
                if (!enqueue(queues, artifact, orchestrationResult)) return orchestrationResult;
            }
        }

        List<ScheduledArtifact> inFlight = new ArrayList<>();
        while (hasQueuedArtifacts(queues) || !inFlight.isEmpty()) {
            DUUIScheduler.Selection selection = scheduler.select(queues, inFlight.size(), executor);
            boolean dispatched = false;
            if (selection != null) {
                DUUIArtifact<?> artifact = selection.artifact();
                DUUIExecutionContext executionContext = contexts.remove(artifact.id());
                if (executionContext == null) {
                    executionContext = initialContext.copyValues();
                }
                DUUITask<DUUIExecutionResult<?>> task = taskUnchecked(selection.checkpoint(), artifact, executionContext);
                scheduler.dispatch(task, executor, executor.dispatchPolicyFor(selection.checkpoint(), artifact));
                inFlight.add(new ScheduledArtifact(selection.checkpoint(), artifact, executionContext, task));
                dispatched = true;
            }

            boolean completed = drainCompleted(
                    inFlight,
                    contexts,
                    queues,
                    orchestrationResult,
                    parkedParents,
                    pendingChildren
            );
            if (completed && orchestrationResult.hasFailures() && config.failFast()) {
                return orchestrationResult;
            }

            if (!dispatched && !completed && !inFlight.isEmpty()) {
                ScheduledArtifact scheduled = inFlight.remove(0);
                if (!completeScheduled(
                        scheduled,
                        contexts,
                        queues,
                        orchestrationResult,
                        parkedParents,
                        pendingChildren
                )) {
                    return orchestrationResult;
                }
            }
        }
        return orchestrationResult;
    }

    private boolean drainCompleted(
            List<ScheduledArtifact> inFlight,
            Map<String, DUUIExecutionContext> contexts,
            Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues,
            DUUIOrchestrationResult orchestrationResult,
            Map<String, DUUIArtifact<?>> parkedParents,
            Map<String, Integer> pendingChildren
    ) {
        boolean completed = false;
        Iterator<ScheduledArtifact> iterator = inFlight.iterator();
        while (iterator.hasNext()) {
            ScheduledArtifact scheduled = iterator.next();
            if (!scheduled.task().isDone()) {
                continue;
            }
            iterator.remove();
            completed = true;
            if (!completeScheduled(scheduled, contexts, queues, orchestrationResult, parkedParents, pendingChildren)) {
                return true;
            }
        }
        return completed;
    }

    private boolean completeScheduled(
            ScheduledArtifact scheduled,
            Map<String, DUUIExecutionContext> contexts,
            Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues,
            DUUIOrchestrationResult orchestrationResult,
            Map<String, DUUIArtifact<?>> parkedParents,
            Map<String, Integer> pendingChildren
    ) {
        DUUIExecutionResult<?> result = scheduled.task().await();
        DUUIExecutionContext executionContext = scheduled.context();
        orchestrationResult.addResult(result);
        List<DUUIArtifact<?>> emittedArtifacts = executionContext.drainEmittedArtifacts();
        if (isSuspendedForFork(result.artifact())) {
            if (emittedArtifacts.isEmpty()) {
                contexts.put(result.artifact().id(), executionContext.copyValues());
                if (!enqueue(queues, result.artifact(), orchestrationResult)) return false;
            } else {
                parkedParents.put(result.artifact().id(), result.artifact());
                pendingChildren.put(result.artifact().id(), emittedArtifacts.size());
                contexts.put(result.artifact().id(), executionContext.copyValues());
            }
        }
        for (DUUIArtifact<?> emitted : emittedArtifacts) {
            contexts.put(emitted.id(), executionContext.copyValues());
            if (!enqueue(queues, emitted, orchestrationResult)) return false;
        }
        if (!isSuspendedForFork(result.artifact())) {
            String parentId = result.artifact().context().parentArtifactId();
            if (parentId != null && pendingChildren.containsKey(parentId)) {
                int remaining = pendingChildren.compute(parentId, (ignored, count) -> count == null ? 0 : count - 1);
                if (remaining <= 0) {
                    pendingChildren.remove(parentId);
                    DUUIArtifact<?> parent = parkedParents.remove(parentId);
                    if (parent != null && !enqueue(queues, parent, orchestrationResult)) return false;
                }
            }
        }
        return result.status() != DUUIExecutionStatus.FAILED || !config.failFast();
    }

    private static boolean isSuspendedForFork(DUUIArtifact<?> artifact) {
        return artifact != null && "true".equals(artifact.metadata().get(DUUIExecutor.SUSPENDED_FOR_FORK));
    }

    private Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> initializeQueues() {
        Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues = new LinkedHashMap<>();
        for (DUUICheckpoint<?> checkpoint : pipeline.checkpoints()) {
            queues.put(checkpoint, new ArrayDeque<>());
        }
        return queues;
    }

    private boolean hasQueuedArtifacts(Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues) {
        return queues.values().stream().anyMatch(queue -> !queue.isEmpty());
    }

    private boolean enqueue(Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues, DUUIArtifact<?> artifact, DUUIOrchestrationResult result) {
        var checkpoint = director.checkpointFor(pipeline, artifact);
        if (checkpoint.isEmpty()) {
            result.addUnroutableArtifact(artifact);
            return !config.stopOnUnroutableArtifact();
        }
        queues.computeIfAbsent(checkpoint.get(), ignored -> new ArrayDeque<>()).add(artifact);
        return true;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private DUUITask<DUUIExecutionResult<?>> taskUnchecked(DUUICheckpoint checkpoint, DUUIArtifact artifact, DUUIExecutionContext context) {
        return executor.task(context, () -> executor.execute(checkpoint, artifact));
    }

    private record ScheduledArtifact(
            DUUICheckpoint<?> checkpoint,
            DUUIArtifact<?> artifact,
            DUUIExecutionContext context,
            DUUITask<DUUIExecutionResult<?>> task
    ) {
    }

    public String orchestratorId() { return executor.orchestratorId(); }
    public DUUIPipeline pipeline() { return pipeline; }
    public DUUIScheduler scheduler() { return scheduler; }
    public DUUIDirector director() { return director; }
    public DUUIExecutor executor() { return executor; }
    public DUUIOrchestratorConfig config() { return config; }
}
