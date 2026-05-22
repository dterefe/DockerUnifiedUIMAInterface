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
import org.texttechnologylab.duui.timelines.DUUIStatus;
import org.texttechnologylab.duui.timelines.Phase;

import java.lang.reflect.Method;
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
        DUUIWorkerRegistry.registerCurrentThread(this.executor.orchestratorId(), DUUIWorkerKind.ORIGIN, true);
    }

    public DUUIOrchestrationResult run(DUUIArtifact<?> artifact) {
        return run(List.of(artifact));
    }

    public DUUIOrchestrationResult run() {
        return run(new DUUIExecutionContext());
    }

    public DUUIOrchestrationResult run(DUUIExecutionContext rootContext) {
        DUUIExecutionContext effectiveRootContext = rootContext == null ? new DUUIExecutionContext() : rootContext;
        DUUIOrchestrationResult result = new DUUIOrchestrationResult();
        Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues = initializeQueues();
        Map<String, DUUIExecutionContext> contexts = new LinkedHashMap<>();
        for (DUUIPipeline.SourceBinding<?> binding : pipeline.sources()) {
            generateUnchecked(binding, queues, contexts, effectiveRootContext);
        }
        return runQueues(queues, contexts, result, effectiveRootContext);
    }

    public DUUIOrchestrationResult run(Collection<DUUIArtifact<?>> artifacts) {
        return run(artifacts, new DUUIExecutionContext());
    }

    public DUUIOrchestrationResult run(Collection<DUUIArtifact<?>> artifacts, DUUIExecutionContext rootContext) {
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
        return runQueues(queues, contexts, result, initialContext);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void generateUnchecked(
            DUUIPipeline.SourceBinding binding,
            Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues,
            Map<String, DUUIExecutionContext> contexts,
            DUUIExecutionContext rootContext
    ) {
        try {
            executor.dispatcher().dispatch(new org.texttechnologylab.duui.timelines.DUUIDispatcher.Invocation<>(
                    SOURCE_PHASE,
                    SOURCE_METHOD,
                    this,
                    List.of(),
                    () -> {
                        binding.source().generate(artifact -> {
                            queues.computeIfAbsent(binding.output(), ignored -> binding.output().queue()).add(artifact);
                            contexts.put(artifact.id(), rootContext.copyValues());
                        });
                        return null;
                    }
            ));
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
                scheduler.dispatch(task, executor, executor.dispatchPolicyFor(selection.checkpoint(), artifact));
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
        List<DUUIArtifact<?>> emittedArtifacts = executionContext.drainEmittedArtifacts();
        DUUIStage<?> stage = scheduled.checkpoint().stage();

        if (result.status() == DUUIExecutionStatus.FAILED) {
            return !config.failFast();
        }

        boolean advanced = false;
        if (stage != null && stage.type() == DUUIStageType.PROCESSOR && stage.output() != null) {
            enqueue(queues, cast(stage.output()), result.artifact(), contexts, executionContext);
            advanced = true;
        } else if (stage != null && stage.type() == DUUIStageType.JOIN) {
            return completeJoinStage(scheduled, result, contexts, queues, orchestrationResult, parents, childToParent, executionContext);
        } else if (stage != null && (stage.type() == DUUIStageType.ADAPTER || stage.type() == DUUIStageType.FORK || stage.type() == DUUIStageType.SPLIT)) {
            for (DUUIArtifact<?> emitted : emittedArtifacts) {
                enqueue(queues, cast(stage.output()), emitted, contexts, executionContext);
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
            DUUIArtifact joined = ((DUUIJoin) stage.operation()).join((List) artifacts);
            enqueue(queues, cast(stage.output()), joined, contexts, executionContext);
            return true;
        } catch (Exception e) {
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
            return !config.stopOnUnroutableArtifact();
        }
        queues.computeIfAbsent(checkpoint.get(), ignored -> new java.util.concurrent.ConcurrentLinkedQueue<>()).add(artifact);
        return true;
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

    @Phase(DUUIStatus.SOURCE)
    private void sourcePhase() {
    }

    private static final Method SOURCE_METHOD = sourceMethod();
    private static final Phase SOURCE_PHASE = SOURCE_METHOD.getAnnotation(Phase.class);

    private static Method sourceMethod() {
        try {
            Method method = DUUIOrchestrator.class.getDeclaredMethod("sourcePhase");
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
