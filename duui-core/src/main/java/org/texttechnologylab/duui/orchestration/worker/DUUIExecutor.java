package org.texttechnologylab.duui.orchestration.worker;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.exception.DUUIExecutionStatus;
import org.texttechnologylab.duui.exception.DUUIFailure;
import org.texttechnologylab.duui.exception.DUUIFailureAction;
import org.texttechnologylab.duui.exception.DUUIFailureClassifier;
import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.orchestration.DUUITask;
import org.texttechnologylab.duui.event.DUUIEventContext;
import org.texttechnologylab.duui.event.DUUIEventScope;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.pipeline.DUUIAdapter;
import org.texttechnologylab.duui.pipeline.DUUIFork;
import org.texttechnologylab.duui.pipeline.DUUISplit;
import org.texttechnologylab.duui.pipeline.DUUITarget;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.pipeline.DUUIExecutionMode;
import org.texttechnologylab.duui.timelines.DUUIDispatcher;
import org.texttechnologylab.duui.timelines.DUUIStatus;
import org.texttechnologylab.duui.timelines.Phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ConcurrentHashMap;

public final class DUUIExecutor implements AutoCloseable {
    private final String orchestratorId;
    private final DUUIFailureClassifier failureClassifier;
    private final DUUIDispatcher dispatcher;
    private final Map<Integer, DUUIPlatformExecutorService> platformExecutors = new ConcurrentHashMap<>();
    private final DUUIVirtualExecutorService virtualExecutor;
    private final DUUIPlatformExecutorService phasePlatformExecutor;

    public DUUIExecutor() {
        this(UUID.randomUUID().toString(), new DUUIFailureClassifier());
    }

    public DUUIExecutor(DUUIFailureClassifier failureClassifier) {
        this(UUID.randomUUID().toString(), failureClassifier);
    }

    public DUUIExecutor(String orchestratorId) {
        this(orchestratorId, new DUUIFailureClassifier());
    }

    public DUUIExecutor(String orchestratorId, DUUIFailureClassifier failureClassifier) {
        this(orchestratorId, failureClassifier, null);
    }

    public DUUIExecutor(String orchestratorId, DUUIFailureClassifier failureClassifier, DUUIDispatcher dispatcher) {
        this.orchestratorId = orchestratorId == null ? UUID.randomUUID().toString() : orchestratorId;
        this.failureClassifier = failureClassifier == null ? new DUUIFailureClassifier() : failureClassifier;
        this.virtualExecutor = new DUUIVirtualExecutorService(this.orchestratorId);
        this.phasePlatformExecutor = new DUUIPlatformExecutorService(
                this.orchestratorId,
                Math.max(1, Runtime.getRuntime().availableProcessors()));
        this.dispatcher = dispatcher == null
                ? new DUUIDispatcher(phaseExecutor(virtualExecutor), phaseExecutor(phasePlatformExecutor))
                : dispatcher;
    }

    public <T> DUUITask<T> task(DUUIExecutionContext context, java.util.concurrent.Callable<T> work) {
        return new DUUITask<>(orchestratorId, context, work);
    }

    public <T> Future<?> submit(DUUITask<T> task) {
        return submit(task, DUUIDispatchPolicy.mixed());
    }

    public <T> Future<?> submit(DUUITask<T> task, DUUIDispatchPolicy dispatchPolicy) {
        DUUIDispatchPolicy policy = dispatchPolicy == null ? DUUIDispatchPolicy.mixed() : dispatchPolicy;
        task.phaseDispatchOverride(policy.mode() == DUUIDispatchMode.CPU || policy.mode() == DUUIDispatchMode.IO ? policy.mode() : null);
        ExecutorService executor = executorFor(policy);
        executor.execute(task);
        return task;
    }

    public <T> DUUITask<T> runInline(DUUITask<T> task) {
        task.run();
        return task;
    }

    public <T> DUUIExecutionResult<T> execute(DUUICheckpoint<T> checkpoint, DUUIArtifact<T> artifact) {
        DUUIStage<T> stage = checkpoint.stage();
        if (stage == null) {
            return DUUIExecutionResult.success(artifact, 0, 1);
        }
        return executeStage(checkpoint, stage, artifact);
    }

    public DUUIDispatchPolicy dispatchPolicyFor(DUUICheckpoint<?> checkpoint, DUUIArtifact<?> artifact) {
        if (checkpoint == null || checkpoint.stage() == null) {
            return DUUIDispatchPolicy.CALLER;
        }
        return checkpoint.stage().dispatchPolicy();
    }

    public <T> DUUIExecutionResult<T> executeStage(
            DUUICheckpoint<T> checkpoint,
            DUUIStage<T> stage,
            DUUIArtifact<T> artifact
    ) {
        long start = System.currentTimeMillis();
        DUUIFailurePolicy policy = resolvePolicy(checkpoint, stage);
        DUUIFailure lastFailure = null;
        DUUIExecutionContext executionContext = currentExecutionContext();
        DUUIEventContext previousContext = executionContext == null ? null : executionContext.eventContext();
        if (executionContext != null) {
            executionContext.eventContext((previousContext == null ? DUUIEventContext.root(orchestratorId, null) : previousContext).toBuilder()
                    .artifactId(artifact.id())
                    .checkpointId(checkpoint.id())
                    .stageId(stage.id())
                    .build());
        }
        DUUIEventService.current().logger("duui.executor").info("Stage execution prepared stage=" + stage.id() + " artifact=" + artifact.id() + " checkpoint=" + checkpoint.id() + " type=" + stage.type() + " components=" + stage.components().size());

        int attempt = 0;
        try {
            for (attempt = 1; attempt <= policy.maxAttempts(); attempt++) {
                long attemptStart = System.currentTimeMillis();
                DUUIEventService.current().logger("duui.executor").info("Stage attempt started stage=" + stage.id() + " artifact=" + artifact.id() + " attempt=" + attempt + "/" + policy.maxAttempts());
                DUUIEventService.current().metric("stage", "duui.stage.attempt", attempt, "count", 0L,
                        java.util.Map.of("stage", stage.id(), "artifact", artifact.id()));
                DUUIEventScope scope = DUUIEventService.current().scope("stage:" + stage.id());
                try {
                    DUUIArtifact<T> processed = processStage(stage, artifact);
                    long duration = System.currentTimeMillis() - start;
                    long attemptDuration = System.currentTimeMillis() - attemptStart;
                    DUUIEventService.current().metric("stage", "duui.stage.attempt_duration_ms", attemptDuration, "milliseconds", attemptDuration,
                            java.util.Map.of("stage", stage.id(), "artifact", artifact.id(), "status", "success"));
                    DUUIEventService.current().metric("stage", "duui.stage.duration_ms", duration, "milliseconds", duration,
                            java.util.Map.of("stage", stage.id(), "artifact", artifact.id(), "status", "success"));
                    DUUIEventService.current().logger("duui.executor").info("Stage completed stage=" + stage.id() + " artifact=" + artifact.id() + " attempt=" + attempt + " duration_ms=" + duration);
                    return DUUIExecutionResult.success(processed, duration, attempt);
                } catch (Exception e) {
                    scope.fail(e);
                    lastFailure = failureClassifier.classify(e, artifact, checkpoint, stage);
                    long attemptDuration = System.currentTimeMillis() - attemptStart;
                    DUUIEventService.current().metric("stage", "duui.stage.attempt_duration_ms", attemptDuration, "milliseconds", attemptDuration,
                            java.util.Map.of("stage", stage.id(), "artifact", artifact.id(), "status", "failed"));
                    DUUIEventService.current().logger("duui.executor").error("Stage attempt failed stage=" + stage.id() + " artifact=" + artifact.id() + " attempt=" + attempt + " action=" + policy.action(), e);
                    if (!shouldRetry(policy, attempt)) {
                        long duration = System.currentTimeMillis() - start;
                        DUUIEventService.current().metric("stage", "duui.stage.duration_ms", duration, "milliseconds", duration,
                                java.util.Map.of("stage", stage.id(), "artifact", artifact.id(), "status", "failed"));
                        return DUUIExecutionResult.failure(artifact, lastFailure, duration, attempt);
                    }
                    DUUIEventService.current().logger("duui.executor").warning("Retrying stage stage=" + stage.id() + " artifact=" + artifact.id() + " next_attempt=" + (attempt + 1));
                    sleepBeforeRetry(policy, attempt);
                } finally {
                    scope.close();
                }
            }

            long duration = System.currentTimeMillis() - start;
            DUUIEventService.current().metric("stage", "duui.stage.duration_ms", duration, "milliseconds", duration,
                    java.util.Map.of("stage", stage.id(), "artifact", artifact.id(), "status", "failed"));
            DUUIEventService.current().logger("duui.executor").error("Stage exhausted attempts stage=" + stage.id() + " artifact=" + artifact.id());
            return DUUIExecutionResult.failure(artifact, lastFailure, duration, attempt);
        } finally {
            if (executionContext != null) {
                executionContext.eventContext(previousContext);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T> DUUIArtifact<T> processStage(DUUIStage<T> stage, DUUIArtifact<T> artifact) throws Exception {
        return switch (stage.type()) {
            case PROCESSOR -> (DUUIArtifact<T>) DUUIExecutorPhaseDispatch.processor(this, stage, artifact);
            case ADAPTER -> (DUUIArtifact<T>) DUUIExecutorPhaseDispatch.adapter(this, stage, artifact);
            case FORK -> (DUUIArtifact<T>) DUUIExecutorPhaseDispatch.fork(this, stage, artifact);
            case SPLIT -> (DUUIArtifact<T>) DUUIExecutorPhaseDispatch.split(this, stage, artifact);
            case TARGET -> (DUUIArtifact<T>) DUUIExecutorPhaseDispatch.target(this, stage, artifact);
            case JOIN -> (DUUIArtifact<T>) DUUIExecutorPhaseDispatch.join(this, stage, artifact);
        };
    }

    private static <T> List<org.texttechnologylab.duui.ems.DUUIActor> actorList(DUUIStage<T> stage, DUUIArtifact<T> artifact) {
        List<org.texttechnologylab.duui.ems.DUUIActor> actors = new ArrayList<>();
        actors.add(artifact);
        if (stage.type() == org.texttechnologylab.duui.pipeline.DUUIStageType.PROCESSOR) {
            actors.addAll(stage.components());
        }
        return actors;
    }

    private static DUUIFailurePolicy resolvePolicy(DUUICheckpoint<?> checkpoint, DUUIStage<?> stage) {
        if (stage.failurePolicy() != null) return stage.failurePolicy();
        if (checkpoint.failurePolicy() != null) return checkpoint.failurePolicy();
        return DUUIFailurePolicy.FAIL_FAST;
    }

    private static boolean shouldRetry(DUUIFailurePolicy policy, int attempt) {
        if (attempt >= policy.maxAttempts()) return false;
        return policy.action() == DUUIFailureAction.RETRY
                || policy.action() == DUUIFailureAction.BACKOFF_AND_RETRY
                || policy.action() == DUUIFailureAction.THROTTLE_AND_RETRY;
    }

    private static void sleepBeforeRetry(DUUIFailurePolicy policy, int attempt) {
        long delay = switch (policy.backoffStrategy()) {
            case NONE -> 0;
            case FIXED -> policy.initialBackoffMs();
            case LINEAR -> policy.initialBackoffMs() * attempt;
            case EXPONENTIAL, EXPONENTIAL_WITH_JITTER, DECORRELATED_JITTER, DEADLINE_BASED ->
                    policy.initialBackoffMs() * (1L << Math.min(20, attempt - 1));
        };
        delay = Math.min(delay, policy.maxBackoffMs());
        if (policy.jitter() && delay > 0) {
            delay = Math.max(1, (long) (Math.random() * delay));
        }
        if (delay <= 0) return;
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static <T> DUUIArtifact<T> processLinear(DUUIStage<T> stage, DUUIArtifact<T> artifact) throws Exception {
        DUUIArtifact<T> current = artifact;
        DUUIExecutionContext executionContext = DUUIWorker.current().requireCurrentTask().context();
        DUUIEventService.current().logger("duui.executor").debug("Linear stage dispatch started stage=" + stage.id() + " components=" + stage.components().size() + " artifact=" + artifact.id());
        for (DUUIComponent<T> component : stage.components()) {
            DUUIEventContext previousContext = executionContext.eventContext();
            if (previousContext != null) {
                executionContext.eventContext(previousContext.toBuilder()
                        .componentId(component.id())
                        .build());
            }
            try {
                DUUIEventService.current().logger("duui.executor").debug("Linear component dispatch component=" + component.id() + " stage=" + stage.id() + " artifact=" + current.id());
                current = component.process(current);
            } finally {
                executionContext.eventContext(previousContext);
            }
        }
        DUUIEventService.current().logger("duui.executor").debug("Linear stage dispatch completed stage=" + stage.id() + " artifact=" + artifact.id());
        return current;
    }

    private <T> DUUIArtifact<T> processParallel(DUUIStage<T> stage, DUUIArtifact<T> artifact) throws Exception {
        int parallelism = stage.dispatchPolicy().parallelism() == null
                ? stage.components().size()
                : Math.max(1, stage.dispatchPolicy().parallelism());
        DUUIDispatchPolicy dispatchPolicy = DUUIDispatchPolicy.of(stage.dispatchPolicy().mode(), parallelism);
        List<DUUITask<DUUIArtifact<T>>> tasks = new ArrayList<>();
        DUUIExecutionContext parentContext = DUUIWorker.current().requireCurrentTask().context();
        DUUIEventService.current().logger("duui.executor").info("Parallel stage dispatch started stage=" + stage.id() + " parallelism=" + parallelism + " components=" + stage.components().size() + " artifact=" + artifact.id());
        for (DUUIComponent<T> component : stage.components()) {
            DUUIExecutionContext childContext = parentContext.copyValues();
            if (parentContext.eventContext() != null) {
                childContext.eventContext(parentContext.eventContext().toBuilder()
                        .trace(parentContext.eventContext().trace().child())
                        .componentId(component.id())
                        .build());
            }
            DUUITask<DUUIArtifact<T>> task = task(childContext, () -> component.process(artifact));
            DUUIEventService.current().logger("duui.executor").debug("Submitting parallel component component=" + component.id() + " task=" + task.id() + " stage=" + stage.id());
            submit(task, dispatchPolicy);
            tasks.add(task);
        }
        DUUIArtifact<T> current = artifact;
        for (DUUITask<DUUIArtifact<T>> task : tasks) {
            DUUIEventService.current().logger("duui.executor").debug("Awaiting parallel component task=" + task.id() + " stage=" + stage.id());
            current = task.await();
            for (DUUIArtifact<?> emitted : task.context().drainEmittedArtifacts()) {
                DUUIEventService.current().logger("duui.executor").debug("Forwarding emitted artifact artifact=" + emitted.id() + " from_task=" + task.id());
                parentContext.emit(emitted);
            }
        }
        DUUIEventService.current().logger("duui.executor").info("Parallel stage dispatch completed stage=" + stage.id() + " artifact=" + artifact.id());
        return current;
    }

    private ExecutorService executorFor(DUUIDispatchPolicy dispatchPolicy) {
        DUUIDispatchPolicy policy = dispatchPolicy == null ? DUUIDispatchPolicy.mixed() : dispatchPolicy;
        if (policy.mode() == DUUIDispatchMode.IO) {
            return virtualExecutor;
        }
        int parallelism = policy.parallelism() == null ? Runtime.getRuntime().availableProcessors() : Math.max(1, policy.parallelism());
        return platformExecutors.computeIfAbsent(parallelism, key -> new DUUIPlatformExecutorService(orchestratorId, key));
    }

    private static DUUIExecutionContext currentExecutionContext() {
        try {
            return DUUIWorker.current().requireCurrentTask().context();
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    public String orchestratorId() { return orchestratorId; }
    public DUUIDispatcher dispatcher() { return dispatcher; }

    @Override
    public void close() {
        virtualExecutor.shutdown();
        phasePlatformExecutor.shutdown();
        for (DUUIPlatformExecutorService executor : platformExecutors.values()) {
            executor.shutdown();
        }
        platformExecutors.clear();
    }

    private Executor phaseExecutor(ExecutorService executor) {
        return command -> {
            DUUITask<?> task = DUUIWorkerRegistry.currentWorker()
                    .map(DUUIWorker::currentTask)
                    .orElse(null);
            executor.execute(() -> {
                DUUIWorker worker = DUUIWorker.current();
                boolean bound = task != null && worker.currentTask() != task;
                if (bound) {
                    worker.bind(task);
                }
                try {
                    command.run();
                } finally {
                    if (bound) {
                        worker.clear(task);
                    }
                }
            });
        };
    }

    @Phase(DUUIStatus.PROCESSOR)
    public Object processor(Object stageValue, Object artifactValue) throws Exception {
        DUUIStage<?> stage = (DUUIStage<?>) stageValue;
        DUUIArtifact<?> artifact = (DUUIArtifact<?>) artifactValue;
        return stage.executionMode() == DUUIExecutionMode.PARALLEL
                ? processParallel((DUUIStage) stage, (DUUIArtifact) artifact)
                : processLinear((DUUIStage) stage, (DUUIArtifact) artifact);
    }

    @Phase(DUUIStatus.ADAPTER)
    public Object adapter(Object stageValue, Object artifactValue) throws Exception {
        DUUIStage<?> stage = (DUUIStage<?>) stageValue;
        DUUIArtifact<?> artifact = (DUUIArtifact<?>) artifactValue;
        DUUIArtifact<?> emitted = ((DUUIAdapter) stage.operation()).adapt(artifact);
        DUUIWorker.current().requireCurrentTask().context().emit(emitted);
        return artifact;
    }

    @Phase(DUUIStatus.FORK)
    public Object fork(Object stageValue, Object artifactValue) throws Exception {
        DUUIStage<?> stage = (DUUIStage<?>) stageValue;
        DUUIArtifact<?> artifact = (DUUIArtifact<?>) artifactValue;
        ((DUUIFork) stage.operation()).fork(artifact, emitted -> DUUIWorker.current().requireCurrentTask().context().emit(emitted));
        return artifact;
    }

    @Phase(DUUIStatus.SPLIT)
    public Object split(Object stageValue, Object artifactValue) throws Exception {
        DUUIStage<?> stage = (DUUIStage<?>) stageValue;
        DUUIArtifact<?> artifact = (DUUIArtifact<?>) artifactValue;
        ((DUUISplit) stage.operation()).split(artifact, emitted -> DUUIWorker.current().requireCurrentTask().context().emit(emitted));
        return artifact;
    }

    @Phase(DUUIStatus.JOIN)
    public Object join(Object stageValue, Object artifactValue) {
        return artifactValue;
    }

    @Phase(DUUIStatus.TARGET)
    public Object target(Object stageValue, Object artifactValue) throws Exception {
        DUUIStage<?> stage = (DUUIStage<?>) stageValue;
        DUUIArtifact<?> artifact = (DUUIArtifact<?>) artifactValue;
        ((DUUITarget) stage.operation()).accept(artifact);
        return artifact;
    }

    private List<org.texttechnologylab.duui.ems.DUUIActor> phaseActors(String phaseMethod, Object[] args) {
        if (args == null || args.length < 2 || !(args[0] instanceof DUUIStage<?> stage) || !(args[1] instanceof DUUIArtifact<?> artifact)) {
            return List.of();
        }
        return actorList((DUUIStage) stage, (DUUIArtifact) artifact);
    }
}
