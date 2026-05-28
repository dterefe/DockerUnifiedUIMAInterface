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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ConcurrentHashMap;

public final class DUUIExecutor implements AutoCloseable {
    private final String orchestratorId;
    private final DUUIFailureClassifier failureClassifier;
    private final DUUIDispatcher dispatcher;
    private final Map<Integer, DUUIPlatformExecutorService> platformExecutors = new ConcurrentHashMap<>();
    private final DUUIVirtualExecutorService virtualExecutor;

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
        this(orchestratorId, failureClassifier, new DUUIDispatcher());
    }

    public DUUIExecutor(String orchestratorId, DUUIFailureClassifier failureClassifier, DUUIDispatcher dispatcher) {
        this.orchestratorId = orchestratorId == null ? UUID.randomUUID().toString() : orchestratorId;
        this.failureClassifier = failureClassifier == null ? new DUUIFailureClassifier() : failureClassifier;
        this.dispatcher = dispatcher == null ? new DUUIDispatcher() : dispatcher;
        this.virtualExecutor = new DUUIVirtualExecutorService(this.orchestratorId);
    }

    public <T> DUUITask<T> task(DUUIExecutionContext context, java.util.concurrent.Callable<T> work) {
        return new DUUITask<>(orchestratorId, context, work);
    }

    public <T> Future<?> submit(DUUITask<T> task) {
        return submit(task, DUUIDispatchPolicy.mixed());
    }

    public <T> Future<?> submit(DUUITask<T> task, DUUIDispatchPolicy dispatchPolicy) {
        ExecutorService executor = executorFor(dispatchPolicy);
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
        DUUIEventService.current().logger("duui.executor").info("Executing stage " + stage.id() + " for artifact " + artifact.id());

        int attempt = 0;
        try {
            for (attempt = 1; attempt <= policy.maxAttempts(); attempt++) {
                DUUIEventScope scope = DUUIEventService.current().scope("stage:" + stage.id());
                try {
                    DUUIArtifact<T> processed = processStage(stage, artifact);
                    long duration = System.currentTimeMillis() - start;
                    return DUUIExecutionResult.success(processed, duration, attempt);
                } catch (Exception e) {
                    scope.fail(e);
                    lastFailure = failureClassifier.classify(e, artifact, checkpoint, stage);
                    if (!shouldRetry(policy, attempt)) {
                        long duration = System.currentTimeMillis() - start;
                        return DUUIExecutionResult.failure(artifact, lastFailure, duration, attempt);
                    }
                    sleepBeforeRetry(policy, attempt);
                } finally {
                    scope.close();
                }
            }

            long duration = System.currentTimeMillis() - start;
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
            case PROCESSOR -> dispatchPhase(PROCESSOR_PHASE, stage, artifact, () -> stage.executionMode() == DUUIExecutionMode.PARALLEL
                    ? processParallel(stage, artifact)
                    : processLinear(stage, artifact));
            case ADAPTER -> dispatchPhase(ADAPTER_PHASE, stage, artifact, () -> {
                DUUIArtifact<?> emitted = ((DUUIAdapter) stage.operation()).adapt(artifact);
                DUUIWorker.current().requireCurrentTask().context().emit(emitted);
                return artifact;
            });
            case FORK -> dispatchPhase(FORK_PHASE, stage, artifact, () -> {
                ((DUUIFork) stage.operation()).fork(artifact, emitted -> DUUIWorker.current().requireCurrentTask().context().emit(emitted));
                return artifact;
            });
            case SPLIT -> dispatchPhase(SPLIT_PHASE, stage, artifact, () -> {
                ((DUUISplit) stage.operation()).split(artifact, emitted -> DUUIWorker.current().requireCurrentTask().context().emit(emitted));
                return artifact;
            });
            case TARGET -> dispatchPhase(TARGET_PHASE, stage, artifact, () -> {
                ((DUUITarget) stage.operation()).accept(artifact);
                return artifact;
            });
            case JOIN -> dispatchPhase(JOIN_PHASE, stage, artifact, () -> artifact);
        };
    }

    private <T> DUUIArtifact<T> dispatchPhase(Method method, DUUIStage<T> stage, DUUIArtifact<T> artifact, java.util.concurrent.Callable<DUUIArtifact<T>> callable) throws Exception {
        return dispatcher.dispatch(new DUUIDispatcher.Invocation<>(
                method.getAnnotation(Phase.class),
                method,
                this,
                actorList(stage, artifact),
                callable
        ));
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
        for (DUUIComponent<T> component : stage.components()) {
            DUUIEventContext previousContext = executionContext.eventContext();
            if (previousContext != null) {
                executionContext.eventContext(previousContext.toBuilder()
                        .componentId(component.id())
                        .build());
            }
            try {
                current = component.process(current);
            } finally {
                executionContext.eventContext(previousContext);
            }
        }
        return current;
    }

    private <T> DUUIArtifact<T> processParallel(DUUIStage<T> stage, DUUIArtifact<T> artifact) throws Exception {
        int parallelism = stage.dispatchPolicy().parallelism() == null
                ? stage.components().size()
                : Math.max(1, stage.dispatchPolicy().parallelism());
        DUUIDispatchPolicy dispatchPolicy = DUUIDispatchPolicy.of(stage.dispatchPolicy().mode(), parallelism);
        List<DUUITask<DUUIArtifact<T>>> tasks = new ArrayList<>();
        DUUIExecutionContext parentContext = DUUIWorker.current().requireCurrentTask().context();
        for (DUUIComponent<T> component : stage.components()) {
            DUUIExecutionContext childContext = parentContext.copyValues();
            if (parentContext.eventContext() != null) {
                childContext.eventContext(parentContext.eventContext().toBuilder()
                        .trace(parentContext.eventContext().trace().child())
                        .componentId(component.id())
                        .build());
            }
            DUUITask<DUUIArtifact<T>> task = task(childContext, () -> component.process(artifact));
            submit(task, dispatchPolicy);
            tasks.add(task);
        }
        DUUIArtifact<T> current = artifact;
        for (DUUITask<DUUIArtifact<T>> task : tasks) {
            current = task.await();
            for (DUUIArtifact<?> emitted : task.context().drainEmittedArtifacts()) {
                parentContext.emit(emitted);
            }
        }
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
        for (DUUIPlatformExecutorService executor : platformExecutors.values()) {
            executor.shutdown();
        }
        platformExecutors.clear();
    }

    @Phase(DUUIStatus.PROCESSOR)
    private void processorPhase() {
    }

    @Phase(DUUIStatus.ADAPTER)
    private void adapterPhase() {
    }

    @Phase(DUUIStatus.FORK)
    private void forkPhase() {
    }

    @Phase(DUUIStatus.SPLIT)
    private void splitPhase() {
    }

    @Phase(DUUIStatus.JOIN)
    private void joinPhase() {
    }

    @Phase(DUUIStatus.TARGET)
    private void targetPhase() {
    }

    private static final Method PROCESSOR_PHASE = phaseMethod("processorPhase");
    private static final Method ADAPTER_PHASE = phaseMethod("adapterPhase");
    private static final Method FORK_PHASE = phaseMethod("forkPhase");
    private static final Method SPLIT_PHASE = phaseMethod("splitPhase");
    private static final Method JOIN_PHASE = phaseMethod("joinPhase");
    private static final Method TARGET_PHASE = phaseMethod("targetPhase");

    private static Method phaseMethod(String name) {
        try {
            Method method = DUUIExecutor.class.getDeclaredMethod(Objects.requireNonNull(name, "name"));
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
