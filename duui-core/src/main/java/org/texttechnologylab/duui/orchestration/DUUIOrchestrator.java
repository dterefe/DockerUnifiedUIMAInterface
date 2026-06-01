package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.exception.DUUIExecutionStatus;
import org.texttechnologylab.duui.exception.DUUIFailure;
import org.texttechnologylab.duui.exception.DUUIFailureAction;
import org.texttechnologylab.duui.exception.DUUIFailureCategory;
import org.texttechnologylab.duui.exception.DUUIFailureSeverity;
import org.texttechnologylab.duui.exception.DUUIRecoverability;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.governance.DUUIGovernor;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDirector;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIScheduler;
import org.texttechnologylab.duui.orchestration.scheduling.DUUITraitDirector;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutionContext;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorkerKind;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorkerRegistry;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.pipeline.DUUIStageType;
import org.texttechnologylab.duui.pipeline.DUUIJoin;
import org.texttechnologylab.duui.timelines.DUUIDispatcher;
import org.texttechnologylab.duui.timelines.DUUIStatus;
import org.texttechnologylab.duui.timelines.Phase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
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
    private final DUUIGovernor governor;

    public DUUIOrchestrator(DUUIPipeline pipeline) {
        this(UUID.randomUUID().toString(), pipeline, new DUUIScheduler(), new DUUITraitDirector(), null, DUUIOrchestratorConfig.DEFAULT);
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
        this.director = director == null ? new DUUITraitDirector() : director;
        this.executor = executor == null ? new DUUIExecutor(this.orchestratorId) : executor;
        this.config = config == null ? DUUIOrchestratorConfig.DEFAULT : config;
        this.governor = this.config.governor();
        DUUIWorkerRegistry.registerCurrentThread(this.executor.orchestratorId(), DUUIWorkerKind.ORIGIN, true);
    }

    public DUUIOrchestrationResult run(DUUIArtifact<?> artifact) {
        return run(List.of(artifact));
    }

    public DUUIOrchestrationResult run() {
        return run(new DUUIExecutionContext());
    }

    public DUUIOrchestrationResult run(DUUIExecutionContext rootContext) {
        long started = System.currentTimeMillis();
        DUUIEventService.current().logger("duui.orchestrator").info("Pipeline run started pipeline=" + pipeline.id() + " mode=sources");
        governor.onRunStarted(orchestratorId(), pipeline, Map.of("mode", "sources", "startedAt", started));
        DUUIExecutionContext effectiveRootContext = rootContext == null ? new DUUIExecutionContext() : rootContext;
        DUUIOrchestrationResult result = new DUUIOrchestrationResult();
        Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues = initializeQueues();
        Map<String, DUUIExecutionContext> contexts = new LinkedHashMap<>();
        for (DUUIPipeline.SourceBinding<?> binding : pipeline.sources()) {
            generateUnchecked(binding, queues, contexts, effectiveRootContext);
        }
        DUUIOrchestrationResult completed = runQueues(queues, contexts, result, effectiveRootContext);
        logRunCompleted(started, completed);
        governor.onRunCompleted(orchestratorId(), pipeline, completed, Map.of("durationMs", System.currentTimeMillis() - started));
        return completed;
    }

    public DUUIOrchestrationResult run(Collection<DUUIArtifact<?>> artifacts) {
        return run(artifacts, new DUUIExecutionContext());
    }

    public DUUIOrchestrationResult run(Collection<DUUIArtifact<?>> artifacts, DUUIExecutionContext rootContext) {
        long started = System.currentTimeMillis();
        int artifactCount = artifacts == null ? 0 : artifacts.size();
        DUUIEventService.current().logger("duui.orchestrator").info("Pipeline run started pipeline=" + pipeline.id() + " artifacts=" + artifactCount);
        governor.onRunStarted(orchestratorId(), pipeline, Map.of("mode", "artifacts", "artifactCount", artifactCount, "startedAt", started));
        DUUIOrchestrationResult result = new DUUIOrchestrationResult();
        DUUIExecutionContext initialContext = rootContext == null ? new DUUIExecutionContext() : rootContext;
        Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues = initializeQueues();
        Map<String, DUUIExecutionContext> contexts = new LinkedHashMap<>();
        if (artifacts != null) {
            for (DUUIArtifact<?> artifact : artifacts.stream().filter(Objects::nonNull).toList()) {
                contexts.put(artifact.id(), initialContext.copyValues());
                if (!enqueueInitial(queues, artifact, result)) return result;
            }
        }
        DUUIOrchestrationResult completed = runQueues(queues, contexts, result, initialContext);
        logRunCompleted(started, completed);
        governor.onRunCompleted(orchestratorId(), pipeline, completed, Map.of("durationMs", System.currentTimeMillis() - started));
        return completed;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void generateUnchecked(
            DUUIPipeline.SourceBinding binding,
            Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues,
            Map<String, DUUIExecutionContext> contexts,
            DUUIExecutionContext rootContext
    ) {
        try {
            DUUIOrchestratorPhaseDispatch.source(this, binding, queues, contexts, rootContext);
        } catch (Exception e) {
            throw new DUUIFrameworkStateException("DUUI source failed before task scheduling.", e);
        }
    }

    private DUUIOrchestrationResult runQueues(
            Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues,
            Map<String, DUUIExecutionContext> contexts,
            DUUIOrchestrationResult orchestrationResult,
            DUUIExecutionContext initialContext
    ) {
        Map<String, ParentJoin> parents = new LinkedHashMap<>();
        Map<String, String> childToParent = new LinkedHashMap<>();
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
                DUUIEventService.current().logger("duui.orchestrator").debug("Scheduling artifact artifact=" + artifact.id() + " checkpoint=" + selection.checkpoint().id() + " task=" + task.id() + " in_flight=" + inFlight.size());
                scheduler.dispatch(task, executor, executor.dispatchPolicyFor(selection.checkpoint(), artifact));
                governor.onTaskScheduled(orchestratorId(), pipeline, artifact, selection.checkpoint(), task, Map.of("inFlight", inFlight.size()));
                inFlight.add(new ScheduledArtifact(selection.checkpoint(), artifact, executionContext, task));
                dispatched = true;
            }

            boolean completed = drainCompleted(inFlight, contexts, queues, orchestrationResult, parents, childToParent);
            if (completed && orchestrationResult.hasFailures() && config.failFast()) {
                return orchestrationResult;
            }

            if (!dispatched && !completed && !inFlight.isEmpty()) {
                ScheduledArtifact scheduled = inFlight.remove(0);
                if (!completeScheduled(scheduled, contexts, queues, orchestrationResult, parents, childToParent)) {
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
            Map<String, ParentJoin> parents,
            Map<String, String> childToParent
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
            if (!completeScheduled(scheduled, contexts, queues, orchestrationResult, parents, childToParent)) {
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
            Map<String, ParentJoin> parents,
            Map<String, String> childToParent
    ) {
        DUUIExecutionResult<?> result = scheduled.task().await();
        DUUIExecutionContext executionContext = scheduled.context();
        orchestrationResult.addResult(result);
        DUUIEventService.current().logger("duui.orchestrator").debug("Completed scheduled artifact artifact=" + scheduled.artifact().id() + " checkpoint=" + scheduled.checkpoint().id() + " task=" + scheduled.task().id() + " status=" + result.status());
        governor.onTaskCompleted(orchestratorId(), pipeline, scheduled.artifact(), scheduled.checkpoint(), result, Map.of("task", scheduled.task().id()));
        DUUIEventService.current().metric("orchestrator", "duui.orchestrator.completed_results", orchestrationResult.results().size(), "count", 0L,
                Map.of("pipeline", pipeline.id(), "status", result.status().name()));
        List<DUUIArtifact<?>> emittedArtifacts = executionContext.drainEmittedArtifacts();
        DUUIStage<?> stage = scheduled.checkpoint().stage();

        if (result.status() == DUUIExecutionStatus.FAILED) {
            DUUIEventService.current().logger("duui.orchestrator").error("Artifact failed artifact=" + result.artifact().id() + " checkpoint=" + scheduled.checkpoint().id());
            governor.onTaskFailed(orchestratorId(), pipeline, scheduled.artifact(), scheduled.checkpoint(), null, Map.of("status", result.status().name()));
            return !config.failFast();
        }

        boolean advanced = false;
        if (stage != null && stage.type() == DUUIStageType.PROCESSOR && stage.output() != null) {
            enqueue(queues, cast(stage.output()), result.artifact(), contexts, executionContext);
            DUUIEventService.current().logger("duui.orchestrator").debug("Advanced artifact artifact=" + result.artifact().id() + " from_stage=" + stage.id() + " to_checkpoint=" + stage.output().id());
            advanced = true;
        } else if (stage != null && stage.type() == DUUIStageType.JOIN) {
            return completeJoinStage(scheduled, result, contexts, queues, orchestrationResult, parents, childToParent, executionContext);
        } else if (stage != null && (stage.type() == DUUIStageType.ADAPTER || stage.type() == DUUIStageType.FORK || stage.type() == DUUIStageType.SPLIT)) {
            for (DUUIArtifact<?> emitted : emittedArtifacts) {
                enqueue(queues, cast(stage.output()), emitted, contexts, executionContext);
                DUUIEventService.current().logger("duui.orchestrator").debug("Queued emitted artifact artifact=" + emitted.id() + " from_stage=" + stage.id() + " to_checkpoint=" + stage.output().id());
            }
            advanced = stage.output() != null && !emittedArtifacts.isEmpty();
            if ((stage.type() == DUUIStageType.FORK || stage.type() == DUUIStageType.SPLIT) && stage.continuation() != null) {
                if (emittedArtifacts.isEmpty()) {
                    enqueue(queues, stage.continuation(), result.artifact(), contexts, executionContext);
                } else {
                    parents.put(result.artifact().id(), new ParentJoin(result.artifact(), stage.continuation(), emittedArtifacts.size(), executionContext.copyValues()));
                    for (DUUIArtifact<?> child : emittedArtifacts) {
                        childToParent.put(child.id(), result.artifact().id());
                    }
                }
            }
        }

        if (advanced) {
            String inheritedParent = childToParent.remove(scheduled.artifact().id());
            if (inheritedParent != null) {
                childToParent.put(result.artifact().id(), inheritedParent);
            }
            return true;
        }

        String parentId = childToParent.remove(result.artifact().id());
        if (parentId != null) {
            ParentJoin parent = parents.get(parentId);
            if (parent != null && parent.completeOne()) {
                parents.remove(parentId);
                enqueue(queues, parent.continuation(), parent.artifact(), contexts, parent.context());
                DUUIEventService.current().logger("duui.orchestrator").debug("Parent join completed parent=" + parent.artifact().id() + " continuation=" + parent.continuation().id());
            }
        }
        return true;
    }

    private boolean completeJoinStage(
            ScheduledArtifact scheduled,
            DUUIExecutionResult<?> result,
            Map<String, DUUIExecutionContext> contexts,
            Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues,
            DUUIOrchestrationResult orchestrationResult,
            Map<String, ParentJoin> parents,
            Map<String, String> childToParent,
            DUUIExecutionContext executionContext
    ) {
        DUUIStage<?> stage = scheduled.checkpoint().stage();
        String parentId = childToParent.remove(result.artifact().id());
        if (parentId == null) {
            return emitJoinResult(stage, List.of(result.artifact()), result.artifact(), contexts, queues, orchestrationResult, executionContext);
        }
        ParentJoin parent = parents.get(parentId);
        if (parent == null) {
            return true;
        }
        parent.addJoined(result.artifact());
        if (!parent.completeOne()) {
            return true;
        }
        parents.remove(parentId);
        if (stage.output() == null) {
            enqueue(queues, parent.continuation(), parent.artifact(), contexts, parent.context());
            return true;
        }
        return emitJoinResult(stage, parent.joined(), result.artifact(), contexts, queues, orchestrationResult, executionContext);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean emitJoinResult(
            DUUIStage<?> stage,
            List<DUUIArtifact<?>> artifacts,
            DUUIArtifact<?> failureArtifact,
            Map<String, DUUIExecutionContext> contexts,
            Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues,
            DUUIOrchestrationResult orchestrationResult,
            DUUIExecutionContext executionContext
    ) {
        if (stage.output() == null) {
            return true;
        }
        long start = System.currentTimeMillis();
        try {
            DUUIEventService.current().logger("duui.orchestrator").info("Join started stage=" + stage.id() + " artifacts=" + artifacts.size());
            DUUIArtifact joined = ((DUUIJoin) stage.operation()).join((List) artifacts);
            enqueue(queues, cast(stage.output()), joined, contexts, executionContext);
            DUUIEventService.current().logger("duui.orchestrator").info("Join completed stage=" + stage.id() + " output_artifact=" + joined.id());
            return true;
        } catch (Exception e) {
            DUUIEventService.current().logger("duui.orchestrator").error("Join failed stage=" + stage.id(), e);
            orchestrationResult.addResult(DUUIExecutionResult.failure(failureArtifact, new DUUIFailure(
                    DUUIFailureCategory.PROGRAMMING_BUG,
                    DUUIFailureSeverity.ERROR,
                    DUUIRecoverability.NON_RETRYABLE,
                    DUUIFailureAction.FAIL_FAST,
                    failureArtifact.id(),
                    null,
                    null,
                    stage.id(),
                    stage.componentId(),
                    null,
                    1,
                    e.getMessage(),
                    e
            ), System.currentTimeMillis() - start, 1));
            return !config.failFast();
        }
    }

    @SuppressWarnings("unchecked")
    private static DUUICheckpoint<DUUIArtifact<?>> cast(DUUICheckpoint<?> checkpoint) {
        return (DUUICheckpoint<DUUIArtifact<?>>) checkpoint;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void enqueue(
            Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues,
            DUUICheckpoint checkpoint,
            DUUIArtifact artifact,
            Map<String, DUUIExecutionContext> contexts,
            DUUIExecutionContext context
    ) {
        queues.computeIfAbsent(checkpoint, ignored -> new java.util.concurrent.ConcurrentLinkedQueue<>()).add(artifact);
        contexts.put(artifact.id(), context.copyValues());
    }

    private boolean enqueueInitial(Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues, DUUIArtifact<?> artifact, DUUIOrchestrationResult result) {
        var checkpoint = director.checkpointFor(pipeline, artifact);
        if (checkpoint.isEmpty()) {
            result.addUnroutableArtifact(artifact);
            DUUIEventService.current().logger("duui.orchestrator").warning("Artifact unroutable artifact=" + artifact.id());
            return !config.stopOnUnroutableArtifact();
        }
        queues.computeIfAbsent(checkpoint.get(), ignored -> new java.util.concurrent.ConcurrentLinkedQueue<>()).add(artifact);
        DUUIEventService.current().logger("duui.orchestrator").debug("Initial artifact queued artifact=" + artifact.id() + " checkpoint=" + checkpoint.get().id());
        governor.onArtifactQueued(orchestratorId(), pipeline, artifact, checkpoint.get(), Map.of("source", "initial"));
        return true;
    }

    private void logRunCompleted(long started, DUUIOrchestrationResult result) {
        long durationMs = System.currentTimeMillis() - started;
        DUUIEventService.current().metric("orchestrator", "duui.orchestrator.run_duration_ms", durationMs, "milliseconds", durationMs,
                Map.of("pipeline", pipeline.id(), "status", result.hasFailures() ? "failed" : "completed"));
        DUUIEventService.current().logger("duui.orchestrator").info("Pipeline run completed pipeline=" + pipeline.id() + " duration_ms=" + durationMs + " results=" + result.results().size() + " failed=" + result.hasFailures());
    }

    private Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> initializeQueues() {
        Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues = new IdentityHashMap<>();
        for (DUUICheckpoint<?> checkpoint : pipeline.checkpoints()) {
            queues.put(checkpoint, new java.util.concurrent.ConcurrentLinkedQueue<>());
        }
        return queues;
    }

    private boolean hasQueuedArtifacts(Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues) {
        return queues.values().stream().anyMatch(queue -> !queue.isEmpty());
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

    private static final class ParentJoin {
        private final DUUIArtifact<?> artifact;
        private final DUUICheckpoint<?> continuation;
        private final DUUIExecutionContext context;
        private final List<DUUIArtifact<?>> joined = new ArrayList<>();
        private int remaining;

        private ParentJoin(DUUIArtifact<?> artifact, DUUICheckpoint<?> continuation, int remaining, DUUIExecutionContext context) {
            this.artifact = artifact;
            this.continuation = continuation;
            this.remaining = remaining;
            this.context = context;
        }

        boolean completeOne() {
            remaining--;
            return remaining <= 0;
        }

        void addJoined(DUUIArtifact<?> artifact) {
            joined.add(artifact);
        }

        List<DUUIArtifact<?>> joined() {
            return List.copyOf(joined);
        }

        DUUIArtifact<?> artifact() { return artifact; }
        DUUICheckpoint<?> continuation() { return continuation; }
        DUUIExecutionContext context() { return context; }
    }

    public String orchestratorId() { return executor.orchestratorId(); }
    public DUUIPipeline pipeline() { return pipeline; }
    public DUUIScheduler scheduler() { return scheduler; }
    public DUUIDirector director() { return director; }
    public DUUIExecutor executor() { return executor; }
    public DUUIDispatcher dispatcher() { return executor.dispatcher(); }

    @Phase(DUUIStatus.SOURCE)
    public void source(
            DUUIPipeline.SourceBinding binding,
            Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues,
            Map<String, DUUIExecutionContext> contexts,
            DUUIExecutionContext rootContext
    ) throws Exception {
        binding.source().generate(artifact -> {
            DUUIEventService.current().logger("duui.orchestrator").debug("Source emitted artifact artifact=" + artifact.id() + " checkpoint=" + binding.output().id());
            queues.computeIfAbsent(binding.output(), ignored -> binding.output().queue()).add(artifact);
            contexts.put(artifact.id(), rootContext.copyValues());
            governor.onArtifactQueued(orchestratorId(), pipeline, artifact, binding.output(), Map.of("source", "pipeline-source"));
        });
    }
}
