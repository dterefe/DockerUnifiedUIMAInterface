# Scheduler — Channel/Signal Primitive Design

## Primitives (exist in duui-base)

```
Signal<T>       — time-stamped value with projection
Channel<S,R>    — typed channel source→result, CompletableFuture<R>
Flow<T>         — CompletableFuture<T> + phase
Stream<T>       — unbounded thread-safe stream, drainable to pool
Projection<T>   — lazy/on-demand value access
Checkpoint<T>   — admission-controlled queue, backpressure modes
Pool<T>         — resource pool with Permit + Lease
```

## Task

A task declares:
- Input signal type
- Output signal type
- Which channels to await before starting

```java
interface StageFunction<I, O> {
    Signal<O> apply(Signal<I> input);
}
```

## Dependencies — Runtime, Not Declared

A task awaits channels at runtime. No pre-declared DAG. No topological sort.

```java
pipeline.signal("C", input -> {
    var a = input.await(channel("A"));   // blocks until A's channel completes
    var b = input.await(channel("B"));
    return compute(a, b);
});
```

## Flow

```
Source discovers → Signal<JCas> enqueued into stage1 checkpoint
Scheduler selects checkpoint → worker dequeues → acquires concurrency slot
Worker calls stageFunction.apply(signal) → completes channel → releases slot
Result Signal propagates to next checkpoint via channel completion
```

## Concurrency

Stage declares model. Pipeline enforces via Semaphore.

```java
.stage("annotate", annotator, Slot.shared())   // 1 at a time
.stage("annotate", annotator, Pool.of(4))      // 4 concurrent
.stage("annotate", annotator, Virtual)          // virtual thread per item
```

## Backpressure

Checkpoint modes. Pipeline enforces. Task never sees it.

```
QUEUE        — accept everything
BACKPRESSURE — block producer when full
STREAMLINE   — reject when downstream saturated
```

## Scheduling

Pure function of checkpoints.

```java
interface SchedulingPolicy {
    int select(Snapshot[] snapshots);  // index or -1 for WAIT
}
```

Snapshot fields: depth, oldestWait, downstreamAvailability, downstreamLatency, downstreamErrors.

Default: deepest queue first, oldest wait tiebreaker.

## User API

```java
var pipeline = Pipeline.define("import")
    .source(FileDiscovery.of("/data/xmi", "*.xmi.gz"))
    .stage("annotate", new NerAnnotator())
        .concurrency(Pool.of(4))
    .stage("write", new LmdbWriter("/store"))
        .concurrency(Slot.shared())
    .onFailure(SKIP_AND_CONTINUE)
    .withTelemetry(Telemetry.toFile("/tmp/telemetry.jsonl"))
    .build();

pipeline.run();
```

User never sees: threads, channels, checkpoints, semaphores, futures, dependencies, backpressure.
