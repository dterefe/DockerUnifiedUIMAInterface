# Scheduler — Channel/Signal Primitive Design

## Core Philosophy

No topological sort of stages. No forced type hierarchy on tasks. The pipeline
provides PRIMITIVES — channels, signals, streams, checkpoints — and the task
integrates with them by declaring what it produces and what it needs. Dependencies
are transparent: a task can `await(channel)` on any channel, not just its immediate
predecessor.

## Primitives (already exist in duui-base)

```
Signal<T>       — time-stamped value with projection semantics
Channel<S,R>    — typed channel source→result, backed by CompletableFuture<R>
Flow<T>         — CompletableFuture<T> + phase tracking
Stream<T>       — unbounded thread-safe stream, drainable to pool
Projection<T>   — lazy/on-demand value access (EXACT, COMPUTED, UNAVAILABLE)
Checkpoint<T>   — admission-controlled queue with backpressure modes
Pool<T>         — resource pool with leasing (Permit + Lease)
```

## How Tasks Work

A Task is simple. It declares:
- What it expects as input (a signal type)
- What it produces as output (a signal type)
- What channels it needs to await before starting
- Optionally, a concurrency model

```java
// DEFINE a stage — just a function from input signal to output signal
// Nothing about plumbing. The pipeline handles channels/checkpoints/pools.
interface StageFunction<I, O> {
    Signal<O> apply(Signal<I> input);
}
```

The pipeline's job: for each task, create a channel, route signals, manage
concurrency, record phases. The task just receives signals and produces signals.

## How Dependencies Work (Transparent, Not Topological)

A task can await ANY channel — not just its immediate predecessor:

```java
// Task C needs both A and B to finish before it starts
// This isn't declared in a DAG — it's in the task itself:
pipeline.signal("C", input -> {
    var aResult = input.await(channel("A"));   // blocks until A's channel completes
    var bResult = input.await(channel("B"));   // blocks until B's channel completes
    return compute(aResult, bResult);
});
```

The pipeline detects that C awaits A and B. It doesn't need a topological sort.
It just knows: "C is waiting on A and B — don't schedule C until both channels
have values."

Dependencies are discovered at RUNTIME, not declared at BUILD time. There is no
dependency graph. There are only channels that tasks wait on.

## How It Actually Works

### Step 1: User defines stages

```java
var pipeline = Pipeline.define("import")
    .stage("read", reader)       // produces Signal<JCas>
    .stage("annotate", annotator) // consumes Signal<JCas>, produces Signal<JCas>
    .stage("write", writer)       // consumes Signal<JCas>, produces Signal<Void>
    .build();
```

### Step 2: Pipeline creates channels

```
read       → channel_read: Channel<Signal<Empty>, Signal<JCas>>
annotate   → channel_annotate: Channel<Signal<JCas>, Signal<JCas>>
write      → channel_write: Channel<Signal<JCas>, Signal<Void>>
```

Each channel has:
- A source projection (what feeds into it)
- A CompletableFuture<R> result (what comes out)
- A checkpoint (buffer of input work items)

### Step 3: Pipeline starts source

The source stage ("read") doesn't await anything. It starts immediately.
It discovers files, creates Signal<JCas> instances, and feeds them into
its output channel. Each signal is enqueued into the annotate stage's
checkpoint.

### Step 4: Pipeline dispatches workers

For each checkpoint with ready work, a worker thread:
1. Dequeues a signal from the checkpoint
2. Calls `stageFunction.apply(signal)`
3. Completes the channel's CompletableFuture with the result
4. The result propagates to the next checkpoint

### Step 5: A task with multiple dependencies

If a task calls `input.await(otherChannel)`, the pipeline detects the
blocking call and records the dependency. The task's worker thread
blocks on the other channel's CompletableFuture. When it completes,
the task resumes.

This is LAZY dependency discovery — no pre-declared DAG.

## Concurrency: The Task Declares, The Pipeline Enforces

```java
// Slot — one at a time, FIFO
pipeline.stage("annotate", annotator, Slot.shared());

// Pool — N concurrent
pipeline.stage("annotate", annotator, Pool.of(4));

// Virtual — one virtual thread per work item
pipeline.stage("annotate", annotator, Virtual);
```

The pipeline creates a Semaphore or executor. Before calling `stageFunction.apply()`,
the worker acquires the semaphore. After, it releases. The task never sees
concurrency control.

## Backpressure: Automatic Via Checkpoint Modes

```
QUEUE        — accept everything, let memory grow (default)
BACKPRESSURE — block producer when checkpoint full
STREAMLINE   — reject when downstream saturated
```

The producer calls `checkpoint.accept(signal)`. The checkpoint's mode determines
whether that call blocks, accepts, or rejects. The task never sees backpressure.

## Scheduling: Policy Receives Snapshots

The scheduler is a pluggable function:

```java
interface SchedulingPolicy {
    int select(Snapshot[] snapshots);
    // returns index of checkpoint to dequeue from, or -1 for WAIT
}
```

Each checkpoint produces a `Snapshot` every time it's queried:
- depth (how many items waiting)
- oldest wait time
- downstream availability (0.0 = saturated, 1.0 = idle)
- downstream latency (rolling average)
- downstream errors

The policy reads these and decides. The default is "deepest queue first,
oldest wait tiebreaker." No global state — policy is a pure function of
current snapshots.

## Complete Signal Chain (End to End)

```
1. Source discovers files → creates Signal<JCas> for each file
2. Signal enqueued into annotate checkpoint
3. Scheduler selects annotate checkpoint (it has work)
4. Worker dequeues signal → acquires concurrency slot
5. Worker calls annotator.apply(signal)
6. Annotator processes JCas → produces new Signal<JCas>
7. Worker completes annotate channel → releases slot
8. Result Signal<JCas> enqueued into write checkpoint
9. Scheduler selects write checkpoint
10. Worker dequeues → calls writer.apply(signal)
11. Writer persists to disk → produces Signal<Void>
12. Worker completes write channel → done
```

Every step is recorded as a phase in the timeline.
Every checkpoint snapshot is available to the telemetry sink.
Every channel completion can be observed.

## What The User Sees

```java
var pipeline = Pipeline.define("biofid")
    .source(FileDiscovery.of("/data/xmi", "*.xmi.gz"))
    .stage("tokenize", new Tokenizer())
        .concurrency(Pool.of(4))
    .stage("ner", new NerAnnotator())
        .concurrency(Virtual)
    .stage("persist", new LmdbWriter("/data/store"))
        .concurrency(Slot.shared())
    .onFailure(SKIP_AND_CONTINUE)
    .withTelemetry(Telemetry.toFile("/tmp/telemetry.jsonl"))
    .withScheduler(new DeepestFirstPolicy())
    .build();

var result = pipeline.run();
System.out.println(result.completed() + " / " + result.total());
```

No threads. No channels. No checkpoints. No semaphores. No CompletableFutures.
No dependency declarations. No topological sort. No type parameters beyond
what the stage functions naturally have.

## What The Pipeline Does (Internally, Invisible to User)

1. Creates a Channel for each stage
2. Creates a Checkpoint for each stage (the input buffer)
3. Connects channels: source output → stage1 checkpoint, stage1 output → stage2 checkpoint, ...
4. Creates a Pool with the declared concurrency model for each stage
5. Creates the scheduler with the declared or default policy
6. Starts the source: discovers inputs, produces signals
7. For each signal in a checkpoint: dispatches a worker
8. Worker runs: acquire slot → apply function → complete channel → release slot
9. Result propagates to next checkpoint via channel completion callback
10. When all source signals are consumed and all channels complete: pipeline done

## Dependencies That Cross Stages (Multi-Input)

When a task needs input from multiple previous stages:

```java
// The task declares what channels it needs
pipeline.stage("merge", (Signal<JCas> input) -> {
    // input is from the immediate predecessor (normal flow)
    // but we also need something from an earlier stage:
    var metadata = input.await(channel("metadata"));
    return merge(input.value(), metadata.value());
});
```

The pipeline detects `await()` calls and records dependencies at runtime.
The task's worker blocks on the awaited channel. No pre-declared DAG.
No forced type compatibility — the task's function signature naturally
expresses what types it expects.

## Summary

| Concept | How It Works |
|---------|--------------|
| Flow between stages | Channels (CompletableFuture-backed) + Checkpoints (buffers) |
| Dependencies | Task calls `await(channel)` — lazy, runtime-discovered |
| Concurrency | Stage declares Slot/Pool/Virtual — pipeline enforces via Semaphore |
| Backpressure | Checkpoint mode: QUEUE / BACKPRESSURE / STREAMLINE |
| Scheduling | Policy is pure function: Snapshot[] → index |
| Telemetry | Sink observes channel completions, checkpoint snapshots, phase events |
| User sees | `.stage(name, function).concurrency(model).build().run()` |
