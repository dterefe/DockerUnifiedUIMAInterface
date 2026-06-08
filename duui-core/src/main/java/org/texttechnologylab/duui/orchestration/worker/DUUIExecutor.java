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
import org.texttechnologylab.duui.pipeline.DUUITarget;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.timelines.DUUIDispatcher;
import org.texttechnologylab.duui.timelines.DUUIFlow;
import org.texttechnologylab.duui.timelines.DUUIStatus;
import org.texttechnologylab.duui.timelines.Phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionException;

public final class DUUIExecutor implements AutoCloseable {
    private static final ConcurrentHashMap<String, DUUIExecutor> INSTANCES = new ConcurrentHashMap<>();

    private final String orchestratorId;
    private final DUUIFailureClassifier failureClassifier;
    private final DUUIDispatcher dispatcher;
    private final Map<Integer, DUUIPlatformExecutor> platformPipelineExecutors = new ConcurrentHashMap<>();
    private final Map<Integer, DUUIPlatformExecutor> platformServiceExecutors = new ConcurrentHashMap<>();
    private final ExecutorService virtualPipelineExecutor;
    private final ExecutorService virtualServiceExecutor;

    public static DUUIExecutor getInstance(String orchestratorId) {
        return INSTANCES.computeIfAbsent(orchestratorId, id -> new DUUIExecutor(id));
    }

    public static DUUIExecutor getInstance(String orchestratorId, DUUIFailureClassifier failureClassifier, DUUIDispatcher dispatcher) {
        return INSTANCES.computeIfAbsent(orchestratorId, id -> new DUUIExecutor(id, failureClassifier, dispatcher));
    }

    private DUUIExecutor(String orchestratorId) {
        this(orchestratorId, new DUUIFailureClassifier(), null);
    }

    private DUUIExecutor(String orchestratorId, DUUIFailureClassifier failureClassifier, DUUIDispatcher dispatcher) {
        this.orchestratorId = orchestratorId == null ? UUID.randomUUID().toString() : orchestratorId;
        this.failureClassifier = failureClassifier == null ? new DUUIFailureClassifier() : failureClassifier;
        this.virtualPipelineExecutor = Executors.newThreadPerTaskExecutor(DUUIWorker.Factory.virtual(this.orchestratorId, DUUIWorker.Type.PIPELINE));
        this.virtualServiceExecutor = Executors.newThreadPerTaskExecutor(DUUIWorker.Factory.virtual(this.orchestratorId, DUUIWorker.Type.SERVICE));
        this.dispatcher = dispatcher == null ? new DUUIDispatcher() : dispatcher;
    }

    public <T> DUUITask<T> task(DUUIExecutionContext context, java.util.concurrent.Callable<T> work) {
        return new DUUITask<>(orchestratorId, context, work);
    }

    public <T> Future<?> submit(DUUITask<T> task) {
        return submit(task, DUUIDispatchPolicy.mixed());
    }

    public <T> Future<?> submit(DUUITask<T> task, DUUIDispatchPolicy dispatchPolicy) {
        DUUIDispatchPolicy policy = dispatchPolicy == null ? DUUIDispatchPolicy.mixed() : dispatchPolicy;
        task.dispatchModeOverride(policy.mode() == DUUIDispatchMode.CPU || policy.mode() == DUUIDispatchMode.IO ? policy.mode() : null);
        ExecutorService executor;
        if (policy.mode() == DUUIDispatchMode.IO) {
            executor = virtualPipelineExecutor;
        } else {
            int parallelism = policy.parallelism() == null ? Runtime.getRuntime().availableProcessors() : Math.max(1, policy.parallelism());
            executor = platformPipelineExecutors.computeIfAbsent(parallelism, key -> new DUUIPlatformExecutor(orchestratorId, DUUIWorker.Type.PIPELINE, key));
        }
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
        DUUIFailurePolicy policy = stage.failurePolicy() != null
                ? stage.failurePolicy()
                : checkpoint.failurePolicy() != null ? checkpoint.failurePolicy() : DUUIFailurePolicy.FAIL_FAST;
        DUUIFailure lastFailure = null;
        DUUIExecutionContext executionContext;
        try {
            executionContext = DUUIWorker.current().requireCurrentTask().context();
        } catch (RuntimeException ignored) {
            executionContext = null;
        }
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
                DUUIEventService.current().logger("duui.executor").info("Stage attempt started stage=" + stage.id() + " artifact=" + artifact.id() + " attempt=" + attempt + "/" + policy.maxAttempts());
                DUUIEventScope scope = DUUIEventService.current().scope("stage:" + stage.id());
                try {
                    DUUIArtifact<T> processed = processStage(stage, artifact);
                    long duration = System.currentTimeMillis() - start;
                    DUUIEventService.current().logger("duui.executor").info("Stage completed stage=" + stage.id() + " artifact=" + artifact.id() + " attempt=" + attempt + " duration_ms=" + duration);
                    return DUUIExecutionResult.success(processed, duration, attempt);
                } catch (Exception e) {
                    scope.fail(e);
                    lastFailure = failureClassifier.classify(e, artifact, checkpoint, stage);
                    DUUIEventService.current().logger("duui.executor").error("Stage attempt failed stage=" + stage.id() + " artifact=" + artifact.id() + " attempt=" + attempt + " action=" + policy.action(), e);
                    if (attempt >= policy.maxAttempts()
                            || (policy.action() != DUUIFailureAction.RETRY
                            && policy.action() != DUUIFailureAction.BACKOFF_AND_RETRY
                            && policy.action() != DUUIFailureAction.THROTTLE_AND_RETRY)) {
                        long duration = System.currentTimeMillis() - start;
                        return DUUIExecutionResult.failure(artifact, lastFailure, duration, attempt);
                    }
                    DUUIEventService.current().logger("duui.executor").warning("Retrying stage stage=" + stage.id() + " artifact=" + artifact.id() + " next_attempt=" + (attempt + 1));
                    sleepBeforeRetry(policy, attempt);
                } finally {
                    scope.close();
                }
            }

            long duration = System.currentTimeMillis() - start;
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
        DUUIFlow<? extends DUUIArtifact<?>> flow = switch (stage.type()) {
            case SOURCE -> sourceFlow(stage, artifact);
            case LINEAR_PROCESSOR, PARALLEL_PROCESSOR -> processor(stage, artifact);
            case ADAPTER -> adapter(stage, artifact);
            case FORK -> fork(stage, artifact);
            case TARGET -> target(stage, artifact);
            case JOIN -> join(stage, artifact);
        };
        return (DUUIArtifact<T>) awaitPhase(flow);
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
                current = (DUUIArtifact<T>) awaitPhase(component.process(current));
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
            DUUITask<DUUIArtifact<T>> task = task(childContext, () -> (DUUIArtifact<T>) awaitPhase(component.process(artifact)));
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

    public String orchestratorId() { return orchestratorId; }
    public DUUIDispatcher dispatcher() { return dispatcher; }

    @Override
    public void close() {
        INSTANCES.remove(orchestratorId);
        virtualPipelineExecutor.shutdown();
        virtualServiceExecutor.shutdown();
        for (DUUIPlatformExecutor executor : platformPipelineExecutors.values()) {
            executor.shutdown();
        }
        platformPipelineExecutors.clear();
        for (DUUIPlatformExecutor executor : platformServiceExecutors.values()) {
            executor.shutdown();
        }
        platformServiceExecutors.clear();
    }

    @Phase(DUUIStatus.PROCESSOR)
    public DUUIFlow<DUUIArtifact<?>> processor(Object stageValue, Object artifactValue) {
        try {
            DUUIStage<?> stage = (DUUIStage<?>) stageValue;
            DUUIArtifact<?> artifact = (DUUIArtifact<?>) artifactValue;
            DUUIArtifact<?> processed = stage.isParallel()
                    ? processParallel((DUUIStage) stage, (DUUIArtifact) artifact)
                    : processLinear((DUUIStage) stage, (DUUIArtifact) artifact);
            return DUUIFlow.dispatch(processed);
        } catch (InterruptedException error) {
            return DUUIFlow.cancel(error);
        } catch (Exception error) {
            return DUUIFlow.fail(error);
        }
    }

    @Phase(DUUIStatus.ADAPTER)
    public DUUIFlow<DUUIArtifact<?>> adapter(Object stageValue, Object artifactValue) {
        try {
            DUUIStage<?> stage = (DUUIStage<?>) stageValue;
            DUUIArtifact<?> artifact = (DUUIArtifact<?>) artifactValue;
            DUUIArtifact<?> emitted = ((DUUIAdapter) stage.operation()).adapt(artifact);
            DUUIWorker.current().requireCurrentTask().context().emit(emitted);
            return DUUIFlow.dispatch(artifact);
        } catch (InterruptedException error) {
            return DUUIFlow.cancel(error);
        } catch (Exception error) {
            return DUUIFlow.fail(error);
        }
    }

    @Phase(DUUIStatus.FORK)
    public DUUIFlow<DUUIArtifact<?>> fork(Object stageValue, Object artifactValue) {
        try {
            DUUIStage<?> stage = (DUUIStage<?>) stageValue;
            DUUIArtifact<?> artifact = (DUUIArtifact<?>) artifactValue;
            ((DUUIFork) stage.operation()).fork(artifact, emitted -> DUUIWorker.current().requireCurrentTask().context().emit(emitted));
            return DUUIFlow.dispatch(artifact);
        } catch (InterruptedException error) {
            return DUUIFlow.cancel(error);
        } catch (Exception error) {
            return DUUIFlow.fail(error);
        }
    }

    @Phase(DUUIStatus.SOURCE)
    public DUUIFlow<DUUIArtifact<?>> sourceFlow(Object stageValue, Object artifactValue) {
        return DUUIFlow.dispatch((DUUIArtifact<?>) artifactValue);
    }

    @Phase(DUUIStatus.JOIN)
    public DUUIFlow<DUUIArtifact<?>> join(Object stageValue, Object artifactValue) {
        return DUUIFlow.dispatch((DUUIArtifact<?>) artifactValue);
    }

    @Phase(DUUIStatus.TARGET)
    public DUUIFlow<DUUIArtifact<?>> target(Object stageValue, Object artifactValue) {
        try {
            DUUIStage<?> stage = (DUUIStage<?>) stageValue;
            DUUIArtifact<?> artifact = (DUUIArtifact<?>) artifactValue;
            ((DUUITarget) stage.operation()).accept(artifact);
            return DUUIFlow.dispatch(artifact);
        } catch (InterruptedException error) {
            return DUUIFlow.cancel(error);
        } catch (Exception error) {
            return DUUIFlow.fail(error);
        }
    }

    private static DUUIArtifact<?> awaitPhase(DUUIFlow<? extends DUUIArtifact<?>> flow) throws Exception {
        try {
            return flow.join();
        } catch (CompletionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error fatal) {
                throw fatal;
            }
            throw error;
        }
    }

}
