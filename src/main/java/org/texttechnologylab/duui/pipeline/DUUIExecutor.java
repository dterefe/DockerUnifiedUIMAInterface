package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.exception.DUUIExecutionStatus;
import org.texttechnologylab.duui.exception.DUUIFailure;
import org.texttechnologylab.duui.exception.DUUIFailureAction;
import org.texttechnologylab.duui.exception.DUUIFailureClassifier;
import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.exception.DUUIFailureSeverity;
import org.texttechnologylab.duui.orchestration.DUUIDispatchMode;
import org.texttechnologylab.duui.orchestration.DUUIDispatchPolicy;
import org.texttechnologylab.duui.orchestration.DUUIExecutionContext;
import org.texttechnologylab.duui.orchestration.DUUIPlatformExecutorService;
import org.texttechnologylab.duui.orchestration.DUUINode;
import org.texttechnologylab.duui.orchestration.DUUITask;
import org.texttechnologylab.duui.orchestration.DUUIVirtualExecutorService;
import org.texttechnologylab.duui.orchestration.DUUIWorker;
import org.texttechnologylab.duui.event.DUUIEventContext;
import org.texttechnologylab.duui.event.DUUIEventScope;
import org.texttechnologylab.duui.event.DUUIEventService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ConcurrentHashMap;

public final class DUUIExecutor implements AutoCloseable {
    public static final String CONTINUATION_STAGE = "duui.continuation.stage";
    public static final String SUSPENDED_FOR_FORK = "duui.suspended.for.fork";
    private final String orchestratorId;
    private final DUUIFailureClassifier failureClassifier;
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
        this.orchestratorId = orchestratorId == null ? UUID.randomUUID().toString() : orchestratorId;
        this.failureClassifier = failureClassifier == null ? new DUUIFailureClassifier() : failureClassifier;
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
        DUUIArtifact<T> current = artifact;
        current.state().checkpointId(checkpoint.id());
        int startStage = continuationStage(current);
        current.metadata().remove(SUSPENDED_FOR_FORK);
        current.metadata().remove(CONTINUATION_STAGE);
        for (int i = startStage; i < checkpoint.stages().size(); i++) {
            DUUIStage<T> stage = checkpoint.stages().get(i);
            DUUIExecutionResult<T> result = executeStage(checkpoint, stage, current, null);
            if (result.status() != DUUIExecutionStatus.SUCCESS) {
                return result;
            }
            current = result.artifact();
            if (stage.forks() && i + 1 < checkpoint.stages().size()) {
                current.metadata().put(SUSPENDED_FOR_FORK, "true");
                current.metadata().put(CONTINUATION_STAGE, Integer.toString(i + 1));
                return DUUIExecutionResult.success(current, 0, current.state().attempt());
            }
        }
        current.state().markComplete();
        return DUUIExecutionResult.success(current, 0, current.state().attempt());
    }

    public DUUIDispatchPolicy dispatchPolicyFor(DUUICheckpoint<?> checkpoint, DUUIArtifact<?> artifact) {
        if (checkpoint == null || checkpoint.stages().isEmpty()) {
            return DUUIDispatchPolicy.CALLER;
        }
        int stageIndex = Math.min(continuationStage(artifact), checkpoint.stages().size() - 1);
        return checkpoint.stages().get(stageIndex).dispatchPolicy();
    }

    public static int continuationStage(DUUIArtifact<?> artifact) {
        String value = artifact.metadata().get(CONTINUATION_STAGE);
        if (value == null || value.isBlank()) return 0;
        try {
            return Math.max(0, Integer.parseInt(value));
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    public <T> DUUIExecutionResult<T> executeStage(
            DUUICheckpoint<T> checkpoint,
            DUUIStage<T> stage,
            DUUIArtifact<T> artifact,
            DUUINode node
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

        try {
            for (int attempt = 1; attempt <= policy.maxAttempts(); attempt++) {
                artifact.state().incrementAttempt();
                if (node != null) node.acquire();
                DUUIEventScope scope = DUUIEventService.current().scope("stage:" + stage.id());
                try {
                    DUUIArtifact<T> processed = stage.type() == DUUIStageType.PARALLEL
                            ? processParallel(stage, artifact)
                            : processLinear(stage, artifact);
                    long duration = System.currentTimeMillis() - start;
                    return DUUIExecutionResult.success(processed, duration, artifact.state().attempt());
                } catch (Exception e) {
                    scope.fail(e);
                    lastFailure = failureClassifier.classify(e, artifact, checkpoint, stage, node);
                    artifact.failures().add(lastFailure);
                    applyFailureSideEffects(artifact, lastFailure);
                    if (!shouldRetry(policy, attempt)) {
                        long duration = System.currentTimeMillis() - start;
                        return DUUIExecutionResult.failure(artifact, lastFailure, duration, artifact.state().attempt());
                    }
                    sleepBeforeRetry(policy, attempt);
                } finally {
                    scope.close();
                    if (node != null) node.release();
                }
            }

            long duration = System.currentTimeMillis() - start;
            return DUUIExecutionResult.failure(artifact, lastFailure, duration, artifact.state().attempt());
        } finally {
            if (executionContext != null) {
                executionContext.eventContext(previousContext);
            }
        }
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
        for (DUUIComponent<T> component : stage.components()) {
            current = component.process(current);
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

    private static void applyFailureSideEffects(DUUIArtifact<?> artifact, DUUIFailure failure) {
        if (failure.severity() == DUUIFailureSeverity.DEGRADED || failure.recommendedAction() == DUUIFailureAction.MARK_DEGRADED) {
            artifact.state().markDegraded();
        }
        if (failure.recommendedAction() == DUUIFailureAction.CANCEL_IMPORT || failure.recommendedAction() == DUUIFailureAction.CHECKPOINT_AND_STOP) {
            artifact.state().markCancelled();
        }
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

    @Override
    public void close() {
        virtualExecutor.shutdown();
        for (DUUIPlatformExecutorService executor : platformExecutors.values()) {
            executor.shutdown();
        }
        platformExecutors.clear();
    }
}
