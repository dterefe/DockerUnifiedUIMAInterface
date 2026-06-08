package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.DUUIPool;
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
import org.texttechnologylab.duui.orchestration.worker.DUUIWorker;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorkerRegistry;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.pipeline.DUUIStageType;
import org.texttechnologylab.duui.pipeline.DUUIJoin;
import org.texttechnologylab.duui.pipeline.DUUISource;
import org.texttechnologylab.duui.timelines.DUUIDispatcher;
import org.texttechnologylab.duui.timelines.DUUIFlow;
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
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class DUUIOrchestrator {
    private final String orchestratorId;
    private final DUUIPipeline pipeline;
    private final DUUIScheduler scheduler;
    private final DUUIDirector director;
    private final DUUIExecutor executor;
    private final DUUIOrchestratorConfig config;
    private final DUUIGovernor governor;
    private final DUUIRuntime runtime;
    public static final ThreadLocal<java.util.function.Supplier<org.apache.uima.jcas.JCas>> MERGED_CAS_SUPPLIER = new ThreadLocal<>();

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
        this.executor = executor == null ? DUUIExecutor.getInstance(this.orchestratorId) : executor;
        this.config = config == null ? DUUIOrchestratorConfig.DEFAULT : config;
        this.governor = this.config.governor();
        this.runtime = DUUIRuntime.getInstance(this.orchestratorId, this.executor);
        DUUIWorkerRegistry.registerCurrentThread(this.executor.orchestratorId(), DUUIWorker.Environment.PLATFORM, DUUIWorker.Type.ORCHESTRATOR);
    }

    /**
     * Creates a {@link DUUIScope} wrapping a new orchestrator.
     * [DESIGN: line 105] — {@code try (DUUIScope<DUUIOrchestrator> orch = DUUIOrchestrator.build())}
     */
    public static DUUIScope<DUUIOrchestrator> build() {
        DUUIPipeline pipeline = DUUIPipeline.builder("default-scope-pipeline")
                .stage(DUUIStage.source("default-source", new org.texttechnologylab.duui.pipeline.DUUISource<>() {
                    @Override
                    public void generate(org.texttechnologylab.duui.artifact.DUUIArtifactEmitter<Object> emitter) {
                    }
                }))
                .stage(DUUIStage.target("default-target", new org.texttechnologylab.duui.pipeline.DUUITarget<>() {
                    @Override
                    public void accept(org.texttechnologylab.duui.artifact.DUUIArtifact<Object> artifact) {
                    }
                }))
                .build();
        DUUIOrchestrator orch = new DUUIOrchestrator(pipeline);
        return new DUUIScope<>(orch, () -> {
            try {
                orch.executor().close();
            } catch (Exception ignored) {
            }
        });
    }

    /**
     * Static factory entry point for building a DUUIOrchestrator.
     * Creates an orchestrator from a DUUIRuntime configuration.
     *
     * <p>This is the primary entry point for pipeline construction.</p>
     *
     * [DESIGN: lines 261-290]
     */
    public static DUUIOrchestrator build(DUUIRuntime runtime) {
        Objects.requireNonNull(runtime, "runtime");
        DUUIOrchestratorConfig config = runtime.pullConfig(DUUIOrchestratorConfig.class);
        if (config == null) {
            config = DUUIOrchestratorConfig.DEFAULT;
        }
        return new DUUIOrchestrator(
                runtime.orchestratorId(),
                runtime.pullConfig(DUUIPipeline.class) != null ? runtime.pullConfig(DUUIPipeline.class) : DUUIPipeline.builder(runtime.orchestratorId()).build(),
                runtime.pullConfig(DUUIScheduler.class) != null ? runtime.pullConfig(DUUIScheduler.class) : new DUUIScheduler(),
                runtime.pullConfig(DUUIDirector.class) != null ? runtime.pullConfig(DUUIDirector.class) : new DUUITraitDirector(),
                runtime.executor(),
                config
        );
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
        // Merge type systems from all component stages before initializing pools
        java.util.function.Supplier<org.apache.uima.jcas.JCas> casSupplier = buildMergedCasSupplier();
        if (casSupplier != null) {
            MERGED_CAS_SUPPLIER.set(casSupplier);
        }
        Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools = initializePools();
        Map<String, DUUIExecutionContext> contexts = new LinkedHashMap<>();
        DUUIStage<?> sourceStage = pipeline.source();
        if (sourceStage != null && sourceStage.operation() instanceof DUUISource<?> src) {
            try {
                sourceFromStage(sourceStage, pools, contexts, effectiveRootContext).join();
            } catch (Exception e) {
                throw new DUUIFrameworkStateException("DUUI source failed before task scheduling.", e);
            }
        }
        DUUIOrchestrationResult completed = runQueues(pools, contexts, result, effectiveRootContext);
        logRunCompleted(started, completed);
        governor.onRunCompleted(orchestratorId(), pipeline, completed, Map.of("durationMs", System.currentTimeMillis() - started));
        return completed;
    }

    private java.util.function.Supplier<org.apache.uima.jcas.JCas> buildMergedCasSupplier() {
        org.apache.uima.resource.metadata.TypeSystemDescription merged = null;
        for (DUUIStage<?> stage : pipeline.stages()) {
            if (stage == null) continue;
            if (stage.components() == null || stage.components().isEmpty()) continue;
            for (Object comp : stage.components()) {
                org.texttechnologylab.duui.pipeline.component.DUUIComponent<?> component = (org.texttechnologylab.duui.pipeline.component.DUUIComponent<?>) comp;
                for (org.texttechnologylab.duui.pipeline.component.DUUINode<?> node : component.nodes()) {
                    org.texttechnologylab.duui.pipeline.component.DUUIAnnotator<?> ann = node.annotator();
                    if (ann instanceof org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator v1) {
                        org.apache.uima.resource.metadata.TypeSystemDescription tsd = v1.typesystem();
                        if (tsd != null) {
                            if (merged == null) {
                                merged = tsd;
                            } else {
                                for (org.apache.uima.resource.metadata.TypeDescription t : tsd.getTypes()) {
                                    merged.addType(t.getName(), t.getDescription(), t.getSupertypeName());
                                }
                            }
                        }
                    }
                }
            }
        }
        if (merged == null) return null;
        final org.apache.uima.resource.metadata.TypeSystemDescription finalTs = merged;
        return () -> {
            try {
                return org.apache.uima.fit.factory.JCasFactory.createJCas(finalTs);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create JCas from merged type system", e);
            }
        };
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
        Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools = initializePools();
        Map<String, DUUIExecutionContext> contexts = new LinkedHashMap<>();
        if (artifacts != null) {
            for (DUUIArtifact<?> artifact : artifacts.stream().filter(Objects::nonNull).toList()) {
                contexts.put(artifact.id(), initialContext.copyValues());
                if (!enqueueInitial(pools, artifact, result)) return result;
            }
        }
        DUUIOrchestrationResult completed = runQueues(pools, contexts, result, initialContext);
        logRunCompleted(started, completed);
        governor.onRunCompleted(orchestratorId(), pipeline, completed, Map.of("durationMs", System.currentTimeMillis() - started));
        return completed;
    }

    private DUUIOrchestrationResult runQueues(
            Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools,
            Map<String, DUUIExecutionContext> contexts,
            DUUIOrchestrationResult orchestrationResult,
            DUUIExecutionContext initialContext
    ) {
        Map<String, ParentJoin> parents = new LinkedHashMap<>();
        Map<String, String> childToParent = new LinkedHashMap<>();
        List<ScheduledArtifact> inFlight = new ArrayList<>();
        while (pools.values().stream().anyMatch(pool -> pool.depth() > 0) || !inFlight.isEmpty()) {
            DUUIScheduler.Selection selection = scheduler.select(pools, inFlight.size(), executor);
            boolean dispatched = false;
            if (selection != null) {
                DUUIArtifact<?> artifact = selection.artifact();
                DUUIExecutionContext executionContext = contexts.remove(artifact.id());
                if (executionContext == null) {
                    executionContext = initialContext.copyValues();
                }
                @SuppressWarnings("rawtypes")
                DUUICheckpoint checkpoint = selection.checkpoint();
                @SuppressWarnings("rawtypes")
                DUUIArtifact rawArtifact = artifact;
                DUUITask<DUUIExecutionResult<?>> task = executor.task(executionContext, () -> executor.execute(checkpoint, rawArtifact));
                ScheduledArtifact scheduled = new ScheduledArtifact(selection.checkpoint(), artifact, executionContext, task);
                attachDUUIFlowRouting(scheduled);
                DUUIEventService.current().logger("duui.orchestrator").debug("Scheduling artifact artifact=" + artifact.id() + " checkpoint=" + selection.checkpoint().id() + " task=" + task.id() + " in_flight=" + inFlight.size());
                scheduler.dispatch(task, executor, executor.dispatchPolicyFor(selection.checkpoint(), artifact));
                governor.onTaskScheduled(orchestratorId(), pipeline, artifact, selection.checkpoint(), task, Map.of("inFlight", inFlight.size()));
                inFlight.add(scheduled);
                dispatched = true;
            }

            boolean completed = drainCompleted(inFlight, contexts, pools, orchestrationResult, parents, childToParent);
            if (completed && orchestrationResult.hasFailures() && config.failFast()) {
                return orchestrationResult;
            }

            if (!dispatched && !completed && !inFlight.isEmpty()) {
                ScheduledArtifact scheduled = inFlight.remove(0);
                scheduled.task().run();
                if (!completeScheduled(scheduled, contexts, pools, orchestrationResult, parents, childToParent)) {
                    return orchestrationResult;
                }
            }
        }
        return orchestrationResult;
    }

    private boolean drainCompleted(
            List<ScheduledArtifact> inFlight,
            Map<String, DUUIExecutionContext> contexts,
            Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools,
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
            if (!completeScheduled(scheduled, contexts, pools, orchestrationResult, parents, childToParent)) {
                return true;
            }
        }
        return completed;
    }

    private boolean completeScheduled(
            ScheduledArtifact scheduled,
            Map<String, DUUIExecutionContext> contexts,
            Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools,
            DUUIOrchestrationResult orchestrationResult,
            Map<String, ParentJoin> parents,
            Map<String, String> childToParent
    ) {
        DUUIExecutionResult<?> result = scheduled.routedResult();
        if (result == null) {
            Throwable routedFailure = scheduled.routedFailure();
            if (scheduled.routedCancelled()) {
                Throwable cancellation = scheduled.routedCancellation();
                result = frameworkFailure(scheduled, cancellation == null
                        ? new CancellationException("Task " + scheduled.task().id() + " was cancelled.")
                        : cancellation, true);
            } else if (routedFailure != null) {
                result = frameworkFailure(scheduled, routedFailure, false);
            } else {
                result = scheduled.task().await();
            }
        }
        DUUIExecutionContext executionContext = scheduled.context();
        orchestrationResult.addResult(result);
        DUUIEventService.current().logger("duui.orchestrator").debug("Completed scheduled artifact artifact=" + scheduled.artifact().id() + " checkpoint=" + scheduled.checkpoint().id() + " task=" + scheduled.task().id() + " status=" + result.status());
        governor.onTaskCompleted(orchestratorId(), pipeline, scheduled.artifact(), scheduled.checkpoint(), result, Map.of("task", scheduled.task().id()));
        List<DUUIArtifact<?>> emittedArtifacts = executionContext.drainEmittedArtifacts();
        DUUIStage<?> stage = scheduled.checkpoint().stage();

        if (result.status() == DUUIExecutionStatus.FAILED) {
            DUUIEventService.current().logger("duui.orchestrator").error("Artifact failed artifact=" + result.artifact().id() + " checkpoint=" + scheduled.checkpoint().id());
            governor.onTaskFailed(orchestratorId(), pipeline, scheduled.artifact(), scheduled.checkpoint(), null, Map.of("status", result.status().name()));
            return !config.failFast();
        }

        boolean advanced = false;
        if (stage != null && stage.isProcessor() && stage.output() != null) {
            enqueue(pools, cast(stage.output()), result.artifact(), contexts, executionContext);
            DUUIEventService.current().logger("duui.orchestrator").debug("Advanced artifact artifact=" + result.artifact().id() + " from_stage=" + stage.id() + " to_checkpoint=" + stage.output().id());
            advanced = true;
        } else if (stage != null && stage.type() == DUUIStageType.JOIN) {
            return completeJoinStage(scheduled, result, contexts, pools, orchestrationResult, parents, childToParent, executionContext);
        } else if (stage != null && (stage.type() == DUUIStageType.ADAPTER || stage.type() == DUUIStageType.FORK)) {
            for (DUUIArtifact<?> emitted : emittedArtifacts) {
                enqueue(pools, cast(stage.output()), emitted, contexts, executionContext);
                DUUIEventService.current().logger("duui.orchestrator").debug("Queued emitted artifact artifact=" + emitted.id() + " from_stage=" + stage.id() + " to_checkpoint=" + stage.output().id());
            }
            advanced = stage.output() != null && !emittedArtifacts.isEmpty();
            if (stage.type() == DUUIStageType.FORK && stage.continuation() != null) {
                if (emittedArtifacts.isEmpty()) {
                    enqueue(pools, stage.continuation(), result.artifact(), contexts, executionContext);
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
                enqueue(pools, parent.continuation(), parent.artifact(), contexts, parent.context());
                DUUIEventService.current().logger("duui.orchestrator").debug("Parent join completed parent=" + parent.artifact().id() + " continuation=" + parent.continuation().id());
            }
        }
        return true;
    }

    private void attachDUUIFlowRouting(ScheduledArtifact scheduled) {
        scheduled.task().flow()
                .onDispatch(() -> DUUIEventService.current().logger("duui.orchestrator").debug(
                        "Dispatched task artifact=" + scheduled.artifact().id()
                                + " checkpoint=" + scheduled.checkpoint().id()
                                + " task=" + scheduled.task().id()))
                .onCompleted(result -> {
                    scheduled.complete(result);
                    DUUIEventService.current().logger("duui.orchestrator").debug(
                            "Task completion event artifact=" + scheduled.artifact().id()
                                    + " checkpoint=" + scheduled.checkpoint().id()
                                    + " task=" + scheduled.task().id());
                })
                .onFailed(error -> {
                    scheduled.fail(error);
                    DUUIEventService.current().logger("duui.orchestrator").error(
                            "Task failure event artifact=" + scheduled.artifact().id()
                                    + " checkpoint=" + scheduled.checkpoint().id()
                                    + " task=" + scheduled.task().id(), error);
                })
                .onCancelled(error -> {
                    scheduled.cancel(error);
                    DUUIEventService.current().logger("duui.orchestrator").warning(
                            "Task cancellation event artifact=" + scheduled.artifact().id()
                                    + " checkpoint=" + scheduled.checkpoint().id()
                                    + " task=" + scheduled.task().id());
                });
    }

    private DUUIExecutionResult<?> frameworkFailure(ScheduledArtifact scheduled, Throwable error, boolean cancelled) {
        DUUIArtifact<?> artifact = scheduled.artifact();
        DUUIStage<?> stage = scheduled.checkpoint().stage();
        DUUIFailure failure = new DUUIFailure(
                cancelled ? DUUIFailureCategory.CANCELLATION : DUUIFailureCategory.PROGRAMMING_BUG,
                cancelled ? DUUIFailureSeverity.WARNING : DUUIFailureSeverity.ERROR,
                DUUIRecoverability.NON_RETRYABLE,
                cancelled ? DUUIFailureAction.CANCEL_IMPORT : DUUIFailureAction.FAIL_FAST,
                artifact.id(),
                artifact.payload() == null ? null : artifact.payload().getClass().getName(),
                scheduled.checkpoint().id(),
                stage == null ? null : stage.id(),
                stage == null ? null : stage.componentId(),
                null,
                1,
                error == null ? null : error.getMessage(),
                error
        );
        return DUUIExecutionResult.failure(artifact, failure, System.currentTimeMillis() - scheduled.startedAt(), 1);
    }

    private boolean completeJoinStage(
            ScheduledArtifact scheduled,
            DUUIExecutionResult<?> result,
            Map<String, DUUIExecutionContext> contexts,
            Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools,
            DUUIOrchestrationResult orchestrationResult,
            Map<String, ParentJoin> parents,
            Map<String, String> childToParent,
            DUUIExecutionContext executionContext
    ) {
        DUUIStage<?> stage = scheduled.checkpoint().stage();
        String parentId = childToParent.remove(result.artifact().id());
        if (parentId == null) {
            return emitJoinResult(stage, List.of(result.artifact()), result.artifact(), contexts, pools, orchestrationResult, executionContext);
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
            enqueue(pools, parent.continuation(), parent.artifact(), contexts, parent.context());
            return true;
        }
        return emitJoinResult(stage, parent.joined(), result.artifact(), contexts, pools, orchestrationResult, executionContext);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean emitJoinResult(
            DUUIStage<?> stage,
            List<DUUIArtifact<?>> artifacts,
            DUUIArtifact<?> failureArtifact,
            Map<String, DUUIExecutionContext> contexts,
            Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools,
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
            enqueue(pools, cast(stage.output()), joined, contexts, executionContext);
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
            Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools,
            DUUICheckpoint checkpoint,
            DUUIArtifact artifact,
            Map<String, DUUIExecutionContext> contexts,
            DUUIExecutionContext context
    ) {
        DUUIPool<DUUIArtifact<?>> pool = pools.get(checkpoint);
        if (pool != null) {
            pool.offer(artifact);
        }
        contexts.put(artifact.id(), context.copyValues());
    }

    private boolean enqueueInitial(Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools, DUUIArtifact<?> artifact, DUUIOrchestrationResult result) {
        var checkpoint = director.checkpointFor(pipeline, artifact);
        if (checkpoint.isEmpty()) {
            result.addUnroutableArtifact(artifact);
            DUUIEventService.current().logger("duui.orchestrator").warning("Artifact unroutable artifact=" + artifact.id());
            return !config.stopOnUnroutableArtifact();
        }
        DUUIPool<DUUIArtifact<?>> pool = pools.get(checkpoint.get());
        if (pool != null) {
            pool.offer(artifact);
        }
        DUUIEventService.current().logger("duui.orchestrator").debug("Initial artifact queued artifact=" + artifact.id() + " checkpoint=" + checkpoint.get().id());
        governor.onArtifactQueued(orchestratorId(), pipeline, artifact, checkpoint.get(), Map.of("source", "initial"));
        return true;
    }

    private void logRunCompleted(long started, DUUIOrchestrationResult result) {
        long durationMs = System.currentTimeMillis() - started;
        DUUIEventService.current().logger("duui.orchestrator").info("Pipeline run completed pipeline=" + pipeline.id() + " duration_ms=" + durationMs + " results=" + result.results().size() + " failed=" + result.hasFailures());
    }

    private Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> initializePools() {
        Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools = new IdentityHashMap<>();
        for (DUUICheckpoint<?> checkpoint : pipeline.checkpoints()) {
            @SuppressWarnings("unchecked")
            DUUIPool<DUUIArtifact<?>> pool = (DUUIPool<DUUIArtifact<?>>) (DUUIPool<?>) checkpoint.pool();
            pools.put(checkpoint, pool);
        }
        return pools;
    }

    @Phase(DUUIStatus.SOURCE)
    public DUUIFlow<Void> sourceFromStage(
            DUUIStage<?> sourceStage,
            Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools,
            Map<String, DUUIExecutionContext> contexts,
            DUUIExecutionContext rootContext
    ) throws Exception {
        DUUISource<?> src = (DUUISource<?>) sourceStage.operation();
        // Source generates artifacts; we find the first processor checkpoint to enqueue into
        List<DUUICheckpoint<?>> checkpoints = pipeline.checkpoints();
        DUUICheckpoint<?> firstCheckpoint = checkpoints.isEmpty() ? null : checkpoints.get(0);
        src.generate(artifact -> {
            DUUIEventService.current().logger("duui.orchestrator").debug("Source emitted artifact artifact=" + artifact.id() + " checkpoint=" + (firstCheckpoint != null ? firstCheckpoint.id() : "none"));
            if (firstCheckpoint != null) {
                DUUIPool<DUUIArtifact<?>> pool = pools.get(firstCheckpoint);
                if (pool != null) {
                    pool.offer(artifact);
                }
            }
            contexts.put(artifact.id(), rootContext.copyValues());
            if (firstCheckpoint != null) {
                governor.onArtifactQueued(orchestratorId(), pipeline, artifact, firstCheckpoint, Map.of("source", "pipeline-source"));
            }
        });
        return DUUIFlow.dispatch();
    }

    private static final class ScheduledArtifact {
        private final DUUICheckpoint<?> checkpoint;
        private final DUUIArtifact<?> artifact;
        private final DUUIExecutionContext context;
        private final DUUITask<DUUIExecutionResult<?>> task;
        private final long startedAt;
        private final AtomicReference<DUUIExecutionResult<?>> routedResult = new AtomicReference<>();
        private final AtomicReference<Throwable> routedFailure = new AtomicReference<>();
        private final AtomicReference<Throwable> routedCancellation = new AtomicReference<>();
        private final AtomicBoolean routedCancelled = new AtomicBoolean(false);

        private ScheduledArtifact(
                DUUICheckpoint<?> checkpoint,
                DUUIArtifact<?> artifact,
                DUUIExecutionContext context,
                DUUITask<DUUIExecutionResult<?>> task
        ) {
            this.checkpoint = checkpoint;
            this.artifact = artifact;
            this.context = context;
            this.task = task;
            this.startedAt = System.currentTimeMillis();
        }

        void complete(DUUIExecutionResult<?> result) {
            routedResult.compareAndSet(null, result);
        }

        void fail(Throwable error) {
            routedFailure.compareAndSet(null, error);
        }

        void cancel(Throwable error) {
            routedCancellation.compareAndSet(null, error);
            routedCancelled.set(true);
        }

        DUUIExecutionResult<?> routedResult() { return routedResult.get(); }
        Throwable routedFailure() { return routedFailure.get(); }
        Throwable routedCancellation() { return routedCancellation.get(); }
        boolean routedCancelled() { return routedCancelled.get(); }
        DUUICheckpoint<?> checkpoint() { return checkpoint; }
        DUUIArtifact<?> artifact() { return artifact; }
        DUUIExecutionContext context() { return context; }
        DUUITask<DUUIExecutionResult<?>> task() { return task; }
        long startedAt() { return startedAt; }
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
    public DUUIRuntime runtime() { return runtime; }
}
