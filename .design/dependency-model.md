# Dependency Model — Task, Signal, Channel Formalization

## Task = Schedulable Unit

DUUITask is `Runnable` + `DUUIActor`. The scheduler needs to evaluate it.
Therefore the task must expose specifications the scheduler can read.

```java
public interface DUUITask<R> extends Runnable, DUUIActor, DUUIProjection<R> {

    // ── Scheduler-visible specifications ──────────────────────

    /** What concurrency model this task requires. */
    DUIConcurrency concurrency();

    /** What execution lane. */
    DUUIExecutionLane lane();

    /** Timeout per execution. */
    Duration timeout();

    /** Retry configuration. */
    RetryConfig retry();

    /** What signals this task AWAITS before it can run. */
    Set<DUUISignal<?>> dependencies();

    /** What signals this task PRODUCES when it completes. */
    Set<DUUISignal<?>> outputs();

    /** What channels this task feeds into (downstream). */
    Set<DUUIChannel<?, ?>> downstream();

    /** Scheduling priority hint. -1 = lowest, 0 = default, +1 = highest. */
    int priority();

    /** Estimated wall-clock cost. Used for load-aware scheduling. */
    Duration estimatedCost();

    /** Whether this task is idempotent (safe to retry without side effects). */
    boolean isIdempotent();

    /** Metadata the scheduler can use for custom policies. */
    Map<String, Object> schedulingAttributes();
}
```

## Dependency Types — Enumerated

A task has dependencies. Dependencies can be of different kinds. The scheduler
treats each kind differently.

```java
/** What kind of dependency this is. */
public enum DependencyKind {

    /** Task cannot run until the signal has a value. Simple data dependency. */
    SIGNAL_VALUE,

    /** Task cannot run until the signal is materialized (projection resolved). */
    SIGNAL_MATERIALIZED,

    /** Task cannot run until ALL signals in a set are ready. */
    SIGNAL_ALL,

    /** Task can run as soon as ANY signal in a set is ready. */
    SIGNAL_ANY,

    /** Task cannot run until the downstream channel has capacity (backpressure). */
    CHANNEL_CAPACITY,

    /** Task cannot run until the downstream channel is idle (no pending work). */
    CHANNEL_IDLE,

    /** Task cannot run until N downstream channels all have capacity. */
    CHANNEL_ALL_CAPACITY,

    /** Task cannot run until the checkpoint depth is below threshold. */
    CHECKPOINT_DEPTH,

    /** Task must run AFTER another task completes (ordering dependency). */
    TASK_COMPLETION,

    /** Task must run AFTER another task fails (error recovery dependency). */
    TASK_FAILURE,

    /** Task cannot start before a wall-clock time. */
    WALL_CLOCK,

    /** No dependencies — task is immediately schedulable. */
    NONE;
}
```

## Dependency Algebra

The scheduler evaluates dependencies to determine schedulability.
Each dependency resolves to a boolean: SATISFIED or NOT_SATISFIED.

```java
public sealed interface Dependency {

    /** What kind of dependency. */
    DependencyKind kind();

    /** Is this dependency currently satisfied? */
    boolean isSatisfied();

    /** How long has this dependency been unsatisfied (for starvation detection). */
    Duration waitDuration();

    // ── Sealed subtypes ───────────────────────────────────────

    /** Wait for a single signal value. */
    record SignalValue(DUUISignal<?> signal) implements Dependency {
        public DependencyKind kind() { return DependencyKind.SIGNAL_VALUE; }
        public boolean isSatisfied() { return signal.available(); }
    }

    /** Wait for ALL signals in a set. */
    record SignalAll(Set<DUUISignal<?>> signals) implements Dependency {
        public DependencyKind kind() { return DependencyKind.SIGNAL_ALL; }
        public boolean isSatisfied() {
            return signals.stream().allMatch(DUUISignal::available);
        }
    }

    /** Wait for ANY signal in a set. */
    record SignalAny(Set<DUUISignal<?>> signals) implements Dependency {
        public DependencyKind kind() { return DependencyKind.SIGNAL_ANY; }
        public boolean isSatisfied() {
            return signals.stream().anyMatch(DUUISignal::available);
        }
    }

    /** Wait for downstream channel capacity. */
    record ChannelCapacity(DUUIChannel<?, ?> channel) implements Dependency {
        public DependencyKind kind() { return DependencyKind.CHANNEL_CAPACITY; }
        public boolean isSatisfied() {
            return channel.downstreamAvailability() > 0.0;
        }
    }

    /** Wait for checkpoint depth below threshold. */
    record CheckpointDepth(DUUICheckpoint<?> checkpoint, int maxDepth)
        implements Dependency {
        public DependencyKind kind() { return DependencyKind.CHECKPOINT_DEPTH; }
        public boolean isSatisfied() {
            return checkpoint.snapshot().depth() <= maxDepth;
        }
    }

    /** Wait for specific task completion. */
    record TaskCompletion(DUUITask<?> task) implements Dependency {
        public DependencyKind kind() { return DependencyKind.TASK_COMPLETION; }
        public boolean isSatisfied() {
            return task.getState() == DUUITaskState.COMPLETED;
        }
    }

    /** Compound: ALL dependencies must be satisfied (AND). */
    record All(List<Dependency> children) implements Dependency {
        public DependencyKind kind() { return DependencyKind.SIGNAL_ALL; }
        public boolean isSatisfied() {
            return children.stream().allMatch(Dependency::isSatisfied);
        }
    }

    /** Compound: ANY dependency must be satisfied (OR). */
    record Any(List<Dependency> children) implements Dependency {
        public DependencyKind kind() { return DependencyKind.SIGNAL_ANY; }
        public boolean isSatisfied() {
            return children.stream().anyMatch(Dependency::isSatisfied);
        }
    }
}
```

## Scheduler Objectives — What to Optimize For

The scheduler evaluates tasks against objectives. Objectives are enumerated —
not arbitrary. The scheduler can only optimize for things it can measure.

```java
/**
 * What the scheduler optimizes for. Each objective has a measurable metric.
 * The scheduler CANNOT optimize for arbitrary goals — only these.
 */
public enum SchedulerObjective {

    // ── THROUGHPUT ─────────────────────────────────

    /** Maximize tasks completed per second. */
    MAXIMIZE_THROUGHPUT {
        public String metric() { return "tasks.completed.per_second"; }
        public boolean isHigherBetter() { return true; }
    },

    /** Maximize total work done (completed + in flight). */
    MAXIMIZE_PROGRESS {
        public String metric() { return "tasks.in_flight"; }
        public boolean isHigherBetter() { return true; }
    },

    // ── LATENCY ────────────────────────────────────

    /** Minimize time from task creation to task start. */
    MINIMIZE_QUEUE_WAIT {
        public String metric() { return "tasks.queue_wait.avg_ms"; }
        public boolean isHigherBetter() { return false; }
    },

    /** Minimize task execution time (prefer fast tasks). */
    MINIMIZE_EXECUTION_TIME {
        public String metric() { return "tasks.execution.avg_ms"; }
        public boolean isHigherBetter() { return false; }
    },

    /** Minimize tail latency (P99). */
    MINIMIZE_TAIL_LATENCY {
        public String metric() { return "tasks.execution.p99_ms"; }
        public boolean isHigherBetter() { return false; }
    },

    // ── FAIRNESS ───────────────────────────────────

    /** Minimize the maximum wait time across all tasks. */
    MINIMIZE_MAX_WAIT {
        public String metric() { return "tasks.wait.max_ms"; }
        public boolean isHigherBetter() { return false; }
    },

    /** Minimize variance in wait times (fairness). */
    MINIMIZE_WAIT_VARIANCE {
        public String metric() { return "tasks.wait.stddev_ms"; }
        public boolean isHigherBetter() { return false; }
    },

    // ── QUALITY ────────────────────────────────────

    /** Maximize completion ratio (completed / total). */
    MAXIMIZE_SUCCESS_YIELD {
        public String metric() { return "tasks.completed.ratio"; }
        public boolean isHigherBetter() { return true; }
    },

    /** Minimize failure ratio. */
    MINIMIZE_FAILURE_RATIO {
        public String metric() { return "tasks.failed.ratio"; }
        public boolean isHigherBetter() { return false; }
    },

    // ── RESOURCE ───────────────────────────────────

    /** Keep heap utilization below limit (default 80%). */
    CONSTRAIN_HEAP_UTILIZATION {
        public String metric() { return "jvm.heap.used_ratio"; }
        public boolean isHigherBetter() { return false; }
    },

    /** Keep CPU utilization below limit. */
    CONSTRAIN_CPU_UTILIZATION {
        public String metric() { return "system.cpu.used_ratio"; }
        public boolean isHigherBetter() { return false; }
    },

    /** Keep downstream channels from saturating. */
    CONSTRAIN_BACKPRESSURE {
        public String metric() { return "channels.saturated.count"; }
        public boolean isHigherBetter() { return false; }
    },

    // ── DEPENDENCY-AWARE ───────────────────────────

    /** Prefer tasks with satisfied dependencies (avoid idle-wait). */
    MAXIMIZE_READY_TASK_RATIO {
        public String metric() { return "tasks.ready.ratio"; }
        public boolean isHigherBetter() { return true; }
    },

    /** Prefer tasks that unblock other tasks (dependency chain length). */
    MAXIMIZE_UNBLOCKING_IMPACT {
        public String metric() { return "tasks.unblocking.count"; }
        public boolean isHigherBetter() { return true; }
    };

    public abstract String metric();
    public abstract boolean isHigherBetter();
}
```

## Scheduler Constraints

Beyond objectives, the scheduler has hard constraints — tasks MUST satisfy
these before being considered.

```java
/**
 * Hard constraints. A task that violates any constraint is NOT schedulable.
 * Unlike objectives (which are soft preferences), constraints are binary.
 */
public enum SchedulerConstraint {

    /** Task dependencies must all be satisfied. */
    DEPENDENCIES_SATISFIED,

    /** Downstream channels must have capacity. */
    DOWNSTREAM_HAS_CAPACITY,

    /** Concurrency slot must be available for this task's concurrency model. */
    CONCURRENCY_SLOT_AVAILABLE,

    /** Heap usage must be below hard limit (separate from soft objective). */
    HEAP_BELOW_HARD_LIMIT,

    /** Task must not have exceeded its retry limit. */
    RETRY_LIMIT_NOT_EXCEEDED,

    /** Task timeout must not have elapsed. */
    TIMEOUT_NOT_EXCEEDED,

    /** If task is non-idempotent, it must not have been attempted already. */
    IDEMPOTENCY_RESPECTED;
}
```

## Scheduler Decision — The Output

The scheduler produces exactly one decision per cycle.

```java
/**
 * The result of one scheduler evaluation cycle.
 * Always exactly one task scheduled, or WAIT.
 */
public sealed interface SchedulingDecision {

    /** Schedule this task for execution. */
    record Schedule(DUUITask<?> task, DUUIExecutionLane lane,
                    DUUIDispatchMode mode, String reason)
        implements SchedulingDecision {}

    /** No task is schedulable right now. Wait for state change. */
    record Wait(Duration suggestedWait, String reason)
        implements SchedulingDecision {}

    /** Pipeline is done — all tasks completed or failed. */
    record Done(PipelineResult result) implements SchedulingDecision {}
}
```

## Task State Machine

```java
public enum DUUITaskState {
    CREATED,        // Task exists, not yet evaluated
    WAITING,        // Dependencies not satisfied — blocked
    SCHEDULABLE,    // Dependencies satisfied, waiting for scheduler
    SCHEDULED,      // Scheduler selected this task
    DISPATCHED,     // Assigned to executor, acquiring concurrency slot
    RUNNING,        // Executing
    COMPLETED,      // Finished successfully
    FAILED,         // Failed (may retry if retries remain)
    RETRYING,       // Waiting for retry backoff
    CANCELLED;      // Explicitly cancelled

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }

    public boolean isActive() {
        return this == RUNNING || this == DISPATCHED;
    }

    public boolean isBlocked() {
        return this == WAITING || this == RETRYING;
    }
}
```

## Complete Algebra — How They Connect

```
Task declares:
  dependencies → Set<Dependency>        (what it waits on)
  concurrency  → DUIConcurrency         (how it executes)
  outputs      → Set<Signal<?>>         (what it produces)
  downstream   → Set<Channel<?,?>>      (where output goes)

Scheduler reads:
  dependencies.isSatisfied()            → can this task run?
  checkpoint.snapshot().depth()         → how much work is waiting?
  checkpoint.snapshot().downstreamAvailability() → is downstream ready?
  task.estimatedCost()                  → how expensive is this?
  task.priority()                       → how urgent?

Scheduler decides:
  Filter: all tasks where constraints pass
  Score:  each task against objectives
  Select: highest-scoring task → Schedule
  Or:     no task passes → Wait
  Or:     all tasks terminal → Done

Constraints (binary, MUST pass):
  DEPENDENCIES_SATISFIED   → task.dependencies().allMatch(Dependency::isSatisfied)
  DOWNSTREAM_HAS_CAPACITY  → task.downstream().allMatch(ch -> ch.downstreamAvailability() > 0)
  CONCURRENCY_SLOT_AVAILABLE → pool.availablePermits() > 0
  HEAP_BELOW_HARD_LIMIT    → Runtime.getRuntime().freeMemory() > threshold
  RETRY_LIMIT_NOT_EXCEEDED → task.retry().attempts() < task.retry().maxAttempts()
  TIMEOUT_NOT_EXCEEDED     → task.elapsed() < task.timeout()
  IDEMPOTENCY_RESPECTED    → task.isIdempotent() || task.retry().attempts() == 0

Objectives (scalar, scored):
  MAXIMIZE_THROUGHPUT      → prefer tasks with short estimatedCost
  MINIMIZE_QUEUE_WAIT      → prefer tasks with high waitDuration
  MAXIMIZE_SUCCESS_YIELD   → prefer tasks from stages with high completion ratio
  CONSTRAIN_HEAP_UTILIZATION → deprioritize if heap > 80%
  MAXIMIZE_UNBLOCKING_IMPACT → prefer tasks whose outputs unblock many dependents
  ...
```

## Signal → Channel Dependency Chain

```
Signal<T> (value at a point in time)
   │
   ├── available() → boolean (is the value known?)
   ├── cached()    → Optional<T> (non-blocking)
   └── materialize() → Flow<T> (future + phase)

Channel<S,R> (connects source signal to result)
   │
   ├── source()             → Projection<S> (input)
   ├── result()             → CompletableFuture<R> (output, set by producer)
   ├── downstreamAvailability() → double (0.0=saturated, 1.0=idle)
   └── snapshot()           → Snapshot (depth, wait, availability, latency, errors)

Dependency (what a task needs before it can run)
   │
   ├── SignalValue(signal)          → wait for one signal value
   ├── SignalAll({s1, s2})          → wait for all signals
   ├── SignalAny({s1, s2})          → wait for any signal
   ├── ChannelCapacity(channel)     → wait for downstream space
   ├── CheckpointDepth(cp, max)     → wait for queue below threshold
   ├── TaskCompletion(task)         → wait for another task
   ├── All({d1, d2})                → AND compound
   └── Any({d1, d2})                → OR compound
```
